from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData").getOrCreate()

# Définir le chemin de base et le chemin du fichier CSV dans des variables
base_path = "/home/nguetcheu"
csv_file_path = "/Documents/projetData/data/GlobalWeatherRepository.csv"

# Charger les données CSV
df_static = spark.read.csv(base_path + csv_file_path, header=True, inferSchema=True)

# Afficher un aperçu des données
df_static.printSchema()

#formatage de la date en format (YYYY-MM-DD)
df_static = df_static.withColumn("last_updated", date_format("last_updated",  "dd-MM-yyyy HH:mm"))


# Transfert des données vers hdfs et sauvegarde en parquet
df_static.write.mode("overwrite").save("hdfs://localhost:9000/user/hadoop/data/climate_data_kaggle")