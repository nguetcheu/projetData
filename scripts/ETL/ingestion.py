
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def ingestion_static_file(spark:SparkSession, input_path:str)->DataFrame:
   
    # Charger les données CSV
    df=spark.read.csv(input_path, header=True, inferSchema=True)

    # Afficher un aperçu des données
    # df.show(10)
    return  df
