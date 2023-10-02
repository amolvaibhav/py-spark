# Setup Oracle Data Base

Prefered CentOS as Bootup OS.
Minimum system requirements to setup DB.
1. Create compute vm (OS - Centos7,  machine type - c3-standard-4,  32 gb , 8VCPUs)
2. Install oracle 19c rpm installation from below link . Download file name - oracle-database-ee-19c-1.0-1.x86_64.rpm
3. Ensure to allow all HTTP traffic on this VM.

Follow this link : https://www.server-world.info/en/note?os=CentOS_7&p=oracle19c&f=6

**initial queries**
1. alter session set "_ORACLE_SCRIPT"=true;
2. create user amol_ap identified by 1234 default tablespace SYSTEM quota UNLIMITED on SYSTEM ;
3. grant create session to amol_ap;
4. grant create session to amol_ap;
5. grant create table to amol_ap;
6. alter user amol_ap quota unlimited on users;


# Setup Dataproc Cluster

gcloud dataproc clusters create spark-clust --region=us-central1

# Secrets

Oracle DB stored in projects/230311958496/secrets/db-secrets/versions/3
Secrets Structure
{"host":<external IP of the VM>, "port":<Port for Oracle Listener>,"service_name":<Service Name setup for the DB>,"user":<DB User>,"password":<password for the DB user>}

# Trigger
Supply the source table, target table and its classification group. The target will be loaded in the respective dataset depending upon the classification group.
Be sure that Oracle DB is up and running dataproc cluster is setup before triggering the script.

python3 testing.py

# Manifest File

source_table,target_table,sensitivity_level
