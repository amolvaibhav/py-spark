'''
gcloud dataproc jobs submit pyspark --cluster=data-extract-cluster --region=us-central1  load_oracle_to_bq.py --jars gs://ap-oracle-to-bq/ojdbc8.jar,gs://ap-oracle-to-bq/spark-bigquery-with-dependencies_2.12-0.32.2.jar -- --source_table=src_emp --target_table=ap_test.tgt_emp --incremental_column=last_updated_time

gcloud dataproc jobs submit pyspark \
  --cluster=data-extract-cluster \
  --region=us-central1 \
  load_oracle_to_bq.py 
  --jars gs://ap-oracle-to-bq/ojdbc8.jar,gs://ap-oracle-to-bq/spark-bigquery-with-dependencies_2.12-0.32.2.jar \
  -- --source_table=src_emp --target_table=ap_test.tgt_emp --incremental_column=last_updated_time



'''


######################################################################################################################
# Script Name    : load_oracle_to_bq.py  
# Script Purpose : This PySpark job extracts the data from on-premises Oracle tables to GCP BigQuery tables 
# Author         : Deloitte 
# Creation Date  : 2023-08-28

# Version Track 
# --------------------------------------------------------------------------------------------------------------------
# Version        Date          Modified by                 Comments
# --------------------------------------------------------------------------------------------------------------------
# 1.0            2023-08-28    Deloitte                    Initial Creation  
#
######################################################################################################################

# Initial imports

import json
import datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StringType,StructType,TimestampType,StructField




# Setting up a SparkSession

spark = SparkSession.builder.appName('Oracle-to-BigQuery-Data-Extract').getOrCreate()


# Setting up Oracle credentials ** ( This needs to be replaced by GCP Secret Manager /Cyberarc fetch )




# Parsing the arguments

parser = argparse.ArgumentParser()
parser.add_argument("--host", required=False, help="Host")
parser.add_argument("--port", required=False, help="Port")
parser.add_argument("--serv", required=False, help="Service Name")
parser.add_argument("--user", required=False, help="User")
parser.add_argument("--pwd", required=False, help="Password")
parser.add_argument("--source_table", required=False, help="Oracle source table name")
parser.add_argument("--target_table", required=False, help="Fully qualified BigQuery table name")
parser.add_argument("--incremental_column", required=False, help="Column in source table on which incremental data is fetch")
args = parser.parse_args()

# Assigning the input parameeters
hst=args.host
prt=args.port
srv=args.serv
usr=args.user
pwd=args.pwd
source_table = args.source_table
target_table = args.target_table
incremental_col = args.incremental_column


driver='oracle.jdbc.driver.OracleDriver'
url=f"jdbc:oracle:thin:@{hst}:{prt}/{srv}"
user=usr
password=pwd
# url=f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/{creds['service_name']}"
# user=creds['user']
# password=creds['password']


print(f"CRED DETAILS : URL={url} user={user} pwd={password}")


# Setting up temp bucket 

bucket = "example-buc-test"
spark.conf.set('temporaryGcsBucket', bucket)


# Forming a blank dataframe of target table. This is to get the schema of target table at run time. Can be done on querying INFORMATION_SCHEMA.COLUMNS view

target_df = spark.read.format('bigquery') \
         .option("table", target_table) \
         .option("filter","1=0") \
         .load()

print("BQ SCHEMA------")
target_df.show()


# Forming the incremental data fetch SQL

sql="select * from src_emp"
#
# incremental_sql = f"""(select * from {source_table}
#                      where {incremental_col} > ( select {incremental_col} from
#                                                    (select {incremental_col}, row_number() over (order by {incremental_col} desc) as row_num
#                                                        from (select distinct {incremental_col}
#                                                                 from {source_table})) where row_num = 2)) t"""
#
#
# print("***********")
# print(incremental_sql)

# Extracting incremental data from source table 

df = spark.read \
    .format('jdbc') \
    .option('driver', driver) \
    .option('url' ,url) \
    .option('dbtable' ,source_table) \
    .option('user' ,user) \
    .option('password' , password) \
    .option("inferSchema","false") \
    .load()

print("Results From SOURCE-------")
df.show()

print(f"Before casting {df.dtypes}")

# for cols in df.columns:
#     df=df.withColumn(cols,col(cols).cast('string'))


#df.withColumn("crt_date",lit(datetime.datetime.now())).withColumn("prcs_name",lit("LOAD_PRCS"))
#
# df.write.option("header",True).option("delimiter","|").csv("gs://example-buc-test/spark_prog/output/run")

# Dynamically changing the source schema into target schemas
#
# target_cols = target_df.columns
# new_df = df.toDF("id","name","city","email","last_upd_dt","crt_date","prcs_name")

#
# # Dynamically changing the datatype of target dataframe to string to make sure it is sync in BigQuery target table schema

for col in df.columns:
     df = df.withColumn(col, df[col].cast(StringType()))

df.printSchema()

new_df=df.select(["*",lit(datetime.datetime.now()).alias("crt_time"),lit("LOAD_PRCS").alias("prcs_name")])


target_cols = target_df.columns
cleaned_df=new_df.toDF(*target_cols)



#cols = [col(s).alias(s.lower()) for s in new_df.columns]
#cleaned_df=new_df.select(lst)
print("Results going to TARGET-------")
cleaned_df.show()
#
#
# # Writing data in BigQuery table

cleaned_df.write.format('com.google.cloud.spark.bigquery').option('table',target_table).mode("overwrite").save()