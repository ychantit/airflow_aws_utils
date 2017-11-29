# airflow_aws_utils
A collection of airflow helper scripts to quickly bootstrap building a data pipeline on aws 

# emr_parallel_job_runner
* This workflow present a handy way to submit jobs in parrallel to an AWS EMR instance.
* I needed a way to submit multiple jobs to a shared EMR instance and execute them in parallel. The AWS EMR Step API only allows to schedule jobs in a sequential way and the AWS DataPipeline is too expensive...
* I ended up using the ssh operator of airflow to connect to the master node of EMR and submit the jobs.
* Assumptions : 
  Aws credentials are correctly set http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
  An Aws emr instance is available
  Airflow is installed https://airflow.apache.org/installation.html
  An airflow connection to connect to the EMR instance is correctly created (ssh_emr_default). This connection holds the reference to the key file for ssh connection, remote user and host to use to connect to your emr instance
