# airflow_aws_utils
A collection of airflow helper scripts to quickly bootstrap building a data pipeline on aws 

# aws_emr_concurrent_job_runner
* This workflow showcase a solution to run concurrent jobs on AWS EMR 
* I needed a way to submit multiple jobs to a shared EMR instance and execute them in parallel. The AWS EMR Step API only allows to schedule jobs in a sequential way and the AWS DataPipeline is too expensive...I ended up using the ssh operator of airflow to connect to the master node of EMR and submit the jobs on cli.
* Assumptions : 
  - Aws credentials are correctly set http://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
  - An Aws emr instance is available
  - Airflow is installed https://airflow.apache.org/installation.html
  - An airflow connection to connect to the EMR instance is correctly created (ssh_emr_default). This connection holds the reference to the key file, remote user and host for ssh session
 * The actual workflow example will compute a number of jobs to run (e.g. as an example partitions to add for a external table on s3) and then launch a sub dag to submit all jobs for aws emr to compute and wait for it to finish

# aws_athena_query_runner
* This workflow shows how to submit a query for aws athena and then block until the query returns
