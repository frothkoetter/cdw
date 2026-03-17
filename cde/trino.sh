# Create a resource
./cde resource create --name cdw-trino-dag 
./cde resource upload --name cdw-trino-dag --local-path cdw-trino-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name cdw-trino-dag-job
./cde job create --name cdw-trino-dag-job --type airflow --dag-file cdw-trino-dag.py  --mount-1-resource cdw-trino-dag 

#Trigger Airflow job to run
./cde job run --name cdw-trino-dag-job 
