# Setup Oracle Data Base

Prefered CentOS as Bootup OS.

Follow this link : https://www.server-world.info/en/note?os=CentOS_7&p=oracle19c&f=6

**initial queries**
alter session set "_ORACLE_SCRIPT"=true;
create user asmit_ap identified by 1234 default tablespace asmit_tbs quota UNLIMITED on asmit_tbs ;
grant create session to asmit_ap;
grant create session to asmit_ap;
grant create table to asmit_ap;
alter user asmit_ap quota unlimited on users;


# Setup Dataproc Cluster

gcloud dataproc clusters create spark-clust --region=us-central1

# Secrets

Oracle DB stored in projects/230311958496/secrets/db-secrets/versions/3

# Trigger

python3 testing.py

# Manifest File

source_table,target_table,sensitivity_level