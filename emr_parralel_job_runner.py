# -*- coding: utf-8 -*-

from datetime import date, timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import ShortCircuitOperator,PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.dummy_operator import DummyOperator
import json 
import boto3
import shutil
import os
import time

# init global vars
defautlt_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# load workflow config
with open('/home/ubuntu/airflow_dag_config/emr_parallel_job_runner.conf') as emr_config:
	wk_conf = json.load(emr_config)
s3_client = boto3.client('s3')

def get_clean_partition_from_resp(response, prefix=''):
	r = []
	for c in response.get('Contents'):
		tmp = c.get('Key').replace(prefix,'')
		p = tmp[:tmp.rfind('/')]
		r.append(p)
	return r
	
	
def get_entity_partitions_to_load(entity):
	json_prefix = wk_conf.get('s3_json_entity_prefix')+entity
	parquet_prefix = wk_conf.get('s3_parquet_entity_prefix')+entity
	response = s3_client.list_objects_v2(Bucket=wk_conf.get('s3_bucket'),Prefix=json_prefix)
	json_partitions = get_clean_partition_from_resp(response,json_prefix)
	response = s3_client.list_objects_v2(Bucket=wk_conf.get('s3_bucket'),Prefix=parquet_prefix)
	parquet_partitions = get_clean_partition_from_resp(response,parquet_prefix)
	# same partitioning required for s3 source and parquet target tables 
	new_partitions = list(set(json_partitions)-set(parquet_partitions))
	return new_partitions
	

def gen_hive_scripts_for_entity(entity):
	parts = get_entity_partitions_to_load(entity)
	hive_script_keys = []
	if len(parts)>0:
		# reset working dir
		working_dir = wk_conf.get('local_working_dir')+'/'+entity
		if not os.path.exists(working_dir):
			os.makedirs(working_dir)
		else:
			shutil.rmtree(working_dir)
			os.makedirs(working_dir)
		entity_template = wk_conf.get('hive_script_template')+'/'+entity+'_parquet_template.hql'
		with open(entity_template, 'r') as temp_file:
			temp = temp_file.read()
		for p in parts:
			t = filter(None,p.split('/'))
			hive_script = temp
			hive_script_name = 'load_parquet_'+entity
			for s in t:
				p_name = s.split('=')[0]
				p_value = s.split('=')[1]
				hive_script = hive_script.replace('__'+p_name+'__',p_value)
				hive_script_name = hive_script_name+'_'+p_name+'_'+p_value
			with open(working_dir+'/'+hive_script_name+'.hql', "w") as v_hive_file:
				v_hive_file.write(hive_script)
			hive_script_key = wk_conf.get('s3_hive_script_location')+'/'+entity+'/'+hive_script_name+'.hql'
			with open(working_dir+'/'+hive_script_name+'.hql', 'rb') as body_file:
				response = s3_client.put_object(Bucket=wk_conf.get('s3_bucket'),
											Key=hive_script_key,
											Body=body_file)
			hive_script_keys.append('s3://'+wk_conf.get('s3_bucket')+'/'+hive_script_key)
	return hive_script_keys

# compute the number of jobs to be run 
# a job is a hive script to load a single partition to a table 
# each job will be running in its own airflow ssh task 
def gen_hive_scripts(**kwargs):
	hive_scripts = []
	hive_steps = []
	# reset s3 hive script location 
	response = s3_client.list_objects_v2(Bucket=wk_conf.get('s3_bucket'),
		Prefix=wk_conf.get('s3_hive_script_location')
		)
	s3_client.delete_objects(Bucket=wk_conf.get('s3_bucket'),
		Delete={'Objects': [{'Key': str(c.get('Key'))} for c in response.get('Contents')]}
		)
	response = s3_client.put_object(
        	Bucket=wk_conf.get('s3_bucket'),
        	Body='',
        	Key=wk_conf.get('s3_hive_script_location')+'/'
        	)
	#
	for entity in wk_conf.get('entities'):
		l = gen_hive_scripts_for_entity(entity)
		if len(l)>0:
			hive_scripts.extend(l)
	i=1
	for f in hive_scripts:
		hive_steps.append({
				'Name': 'convert_s3_json_to_parquet_'+str(i),
				'ActionOnFailure': 'CONTINUE',
				'HadoopJarStep': {
					'Jar': 'command-runner.jar',
					'Args': [
						 'hive','-f', f
					]}})
		i+=1
	kwargs['ti'].xcom_push(key='hive_steps', value=hive_steps)
	if len(hive_steps)>0:
		return True
	return False 
		
# generate a sub dag to submit parallel job to an emr cluster
def get_sub_ssh_cmds_dag(parent_dag, task_id, args):
	ssh_dag = DAG(
		'%s.%s' % (parent_dag.dag_id, task_id),
		default_args=args,
		start_date=args['start_date'],
		schedule_interval=parent_dag.schedule_interval,
		)
	start = DummyOperator(
		task_id='ssh_start',
		dag=ssh_dag)
	end = DummyOperator(
		task_id='ssh_end',
		dag=ssh_dag)
	# generate the task to submit dynamically depending on the number of hive script that needs to be run 
	response = s3_client.list_objects_v2(Bucket=wk_conf.get('s3_bucket'),Prefix=wk_conf.get('s3_hive_script_location'))
	hive_scripts = [c.get('Key') for c in response.get('Contents')]
	if len(hive_scripts)>0:
		ssh_emr_hook = SSHHook(conn_id='ssh_emr_default')
		ssh_tasks = [ SSHExecuteOperator(
			task_id=str(key.replace(':','_').replace('/','_')),
            		ssh_hook=ssh_emr_hook,
            		bash_command='hive -f "s3://'+wk_conf.get('s3_bucket')+'/'+str(key)+'"',
			dag=ssh_dag) for key in hive_scripts if key.endswith('hql')]
	start.set_downstream(ssh_tasks)
	end.set_upstream(ssh_tasks)
	# if no hive scripts generrated short circuit step in the begining of main dag
	return ssh_dag
	
		
dag = DAG(
    's3_convert_json_to_parquet_emr_ssh',
    default_args=defautlt_args,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval='0 3 * * *'
)

step_entities_partitions = ShortCircuitOperator(
		task_id='step_entities_partitions', 
		python_callable=gen_hive_scripts, 
		provide_context=True,
		dag=dag)
		
step_ssh_subdag = SubDagOperator(
	task_id='step_jobs_submit',
	subdag=get_sub_ssh_cmds_dag(dag, 'step_jobs_submit',defautlt_args),
	default_args=defautlt_args,
	dag=dag)
	
step_end = DummyOperator(
	task_id='ssh_end',
	dag=dag)

step_entities_partitions.set_downstream(step_ssh_subdag)
step_ssh_subdag.set_downstream(step_end)
