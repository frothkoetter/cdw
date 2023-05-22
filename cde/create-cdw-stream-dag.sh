# Create a resource
./cde resource create --name cdw-stream-dag 
./cde resource upload --name cdw-stream-dag --local-path cdw-stream-dag.py

# Create Job of “airflow” type and reference the DAG
#./cde job delete --name cdw-stream-dag-job
./cde job create --name cdw-stream-dag-job --type airflow --dag-file cdw-stream-dag.py  --mount-1-resource cdw-stream-dag 

#Trigger Airflow job to run
#./cde job run --name cdw-stream-dag-job 

