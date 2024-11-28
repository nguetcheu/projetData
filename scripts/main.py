from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import os
from ETL.load import load_to_hdfs
from ETL.ingestion import ingestion_static_file
from ETL.transform import set_default_data, transform_date_format, format_columns_names
from dotenv import load_dotenv

 
#Chargement des variable environnements de développement
load_dotenv(dotenv_path=".env.development")

# Définir le chemin de base
base_path= os.environ["BASE_PATH"]


# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData").getOrCreate()


# ----------------------INGESTION-------------------------------------------------------

csv_file_path=os.environ["CSV_FILE_PATH"]
df:DataFrame=ingestion_static_file(spark, input_path=base_path+csv_file_path)


# ----------------------TRANSFORM-------------------------------------------------------

df=format_columns_names(df)

df=set_default_data(df)

df=transform_date_format(df)

# --------------------------LOAD -----------------------------------------------

output_path=os.environ["OUTPUT_FILE"]
load_to_hdfs(df, output_path=output_path)



