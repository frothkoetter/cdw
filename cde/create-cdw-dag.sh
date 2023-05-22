# Create a resource
./cde resource create --name cdw-etl-dag 
./cde resource upload --name cdw-etl-dag --local-path cdw-etl-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name cdw-etl-dag-job
./cde job create --name cdw-etl-dag-job --type airflow --dag-file cdw-etl-dag.py  --mount-1-resource cdw-etl-dag 

#Trigger Airflow job to run
./cde job run --name cdw-etl-dag-job 

