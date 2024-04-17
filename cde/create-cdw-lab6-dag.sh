# Create a resource
./cde resource create --name cdw-lab6-dag 
./cde resource upload --name cdw-lab6-dag --local-path cdw-lab6-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name cdw-lab6-dag-job
./cde job create --name cdw-lab6-dag-job --type airflow --dag-file cdw-lab6-dag.py  --mount-1-resource cdw-lab6-dag 

#Trigger Airflow job to run
./cde job run --name cdw-lab6-dag-job 
