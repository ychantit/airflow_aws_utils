# airflow_aws_utils
A collection of airflow helper scripts to bootstrap building data processing pipelines on aws 

# requirements
  - AWS credentials are correctly set http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
  - An AWS EMR instance is available
  - Airflow is installed https://airflow.apache.org/installation.html
  - An airflow connection to connect to the AWS EMR instance (called ssh_emr_default in the script). This connection holds defines the ssh key file, remote user and host for ssh session, ssh properties (such as no host check)
  
# aws_emr_concurrent_job_runner
* This workflow showcase a solution to run concurrent jobs (such as spark job, hive script, mr job...etc.) on AWS EMR 
* I needed a way to submit multiple jobs to a shared EMR instance and execute them in parallel. The AWS EMR Step API only allows to schedule jobs in a sequential way and the AWS DataPipeline is too expensive...I ended up using the ssh operator of airflow to connect to the master node of EMR and submit the jobs on cli.
* This workflow concurrent jobs are hive scripts. Each script will attempts to write a new partition of an external table stored on S3 in parquet format

# aws_athena_query_runner
* This workflow shows how to submit a query for aws athena and then block until the query returns
