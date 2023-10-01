import datetime
import json
import os
from datetime import timedelta
import csv
import itertools
import random
from google.cloud import bigquery
from google.cloud import secretmanager
import uuid


def main(argv=None):
    client = secretmanager.SecretManagerServiceClient()
    rsrc_name = "projects/230311958496/secrets/db-secrets/versions/3"
    response = client.access_secret_version(request={"name": rsrc_name})
    payload = response.payload.data.decode("UTF-8")
    creds=json.loads(payload)
    metadata = {"P": "protect", "S": "sensitive"}

    # read the manifest file
    with open("C:\\Users\\amvaibhav\\PycharmProjects\\testingpy\\manifest.txt", "r") as conf_file:
        line = conf_file.readlines()

    for tbl in line:
        src_tbl, tgt_tbl, clas = tbl.replace("\n", "").split(",")
        ds = metadata[clas]
        job_submit_command = f"gcloud dataproc jobs submit pyspark --cluster=spark-clust --region=us-central1 " \
                             f"--jars=gs://example-buc-test/ojdbc8.jar " \
                             f"C:\\Users\\amvaibhav\\Downloads\\load_oracle_to_bq.py -- --source_table={src_tbl} " \
                             f"--target_table={ds}.{tgt_tbl.split('.')[1]} --host={creds['host']} --port={creds['port']} --serv={creds['service_name']} --user={creds['user']} --pwd={creds['password']}"
        print(job_submit_command)
        os.system(job_submit_command)

        # print(src_tbl)
        # print(tgt_tbl)
        # print(clas)


if __name__ == "__main__":
    main()
