# Create a resource
./cde resource create --name cdw-lab7-dag 
./cde resource upload --name cdw-lab7-dag --local-path cdw-lab7-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name cdw-lab7-dag-job
./cde job create --name cdw-lab7-dag-job --type airflow --dag-file cdw-lab7-dag.py  --mount-1-resource cdw-lab7-dag 

#Trigger Airflow job to run
./cde job run --name cdw-lab7-dag-job 
