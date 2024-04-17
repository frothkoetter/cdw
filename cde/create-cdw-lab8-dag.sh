# Create a resource
./cde resource create --name cdw-lab8-dag 
./cde resource upload --name cdw-lab8-dag --local-path cdw-lab8-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name cdw-lab8-dag-job
./cde job create --name cdw-lab8-dag-job --type airflow --dag-file cdw-lab8-dag.py  --mount-1-resource cdw-lab8-dag 

#Trigger Airflow job to run
./cde job run --name cdw-lab8-dag-job 
