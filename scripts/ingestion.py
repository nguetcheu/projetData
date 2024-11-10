from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
import os
from dotenv import load_dotenv

#Chargement des variable environnements de développement
load_dotenv(dotenv_path=".env.development")

# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData").getOrCreate()

# Définir le chemin de base et le chemin du fichier CSV dans des variables
base_path = os.environ["BASE_PATH"]
csv_file_path = os.environ["CSV_FILE_PATH"]


# Charger les données CSV
df_static = spark.read.csv(base_path + csv_file_path, header=True, inferSchema=True)

# Afficher un aperçu des données
# df_static.printSchema()
# df_static.show(10)

#format de la date
df_static=df_static.withColumn("last_updated", date_format(col("last_updated"), "dd-MM-yyy HH:mm"))


# Transfert des données vers hdfs
df_static.write.mode("overwrite").save(os.environ["OUTPUT_FILE"])
