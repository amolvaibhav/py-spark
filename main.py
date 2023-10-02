import datetime
import json
import os
from datetime import timedelta
import csv
import itertools
import argparse
from google.cloud import bigquery
from google.cloud import secretmanager
import uuid


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_maifest_path", required=False, help="Local Path for manifest file")
    parser.add_argument("--secret_rsrc", required=False, help="Secret Manager Resource")
    parser.add_argument("--cluster", required=False, help="Spark Cluster")
    args = parser.parse_args()

    client = secretmanager.SecretManagerServiceClient()
    rsrc=args.secret_rsrc
    response = client.access_secret_version(request={"name": rsrc})
    payload = response.payload.data.decode("UTF-8")
    creds=json.loads(payload)
    metadata = {"P": "protect", "S": "sensitive"}

    # read the manifest file
    #with open(local_maifest_path,"r") as conf_file:
    with open("C:\\Users\\amvaibhav\\PycharmProjects\\testingpy\\manifest.txt", "r") as conf_file:
        line = conf_file.readlines()

    for tbl in line:
        src_tbl, tgt_tbl, clas = tbl.replace("\n", "").split(",")
        ds = metadata[clas]
        job_submit_command = f"gcloud dataproc jobs submit pyspark --cluster={args.cluster} --region=us-central1 " \
                             f"--jars=gs://example-buc-test/ojdbc8.jar " \
                             f"C:\\Users\\amvaibhav\\Downloads\\load_oracle_to_bq.py -- --source_table={src_tbl} " \
                             f"--target_table={ds}.{tgt_tbl.split('.')[1]} --host={creds['host']} --port={creds['port']} --serv={creds['service_name']} --user={creds['user']} --pwd={creds['password']}"
        print(job_submit_command)
        os.system(job_submit_command)

if __name__ == "__main__":
    main()




## python3 main.py --local_maifest_path=C:\\Users\\amvaibhav\\PycharmProjects\\testingpy\\manifest.txt --secret_rsrc=projects/230311958496/secrets/db-secrets/versions/3 --cluster=spark-clust
