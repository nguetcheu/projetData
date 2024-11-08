from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData").getOrCreate()

# Définir le chemin de base et le chemin du fichier CSV dans des variables
base_path = "/home/nguetcheu"
csv_file_path = "/Documents/projetData/data/GlobalWeatherRepository.csv"

# Charger les données CSV
df_static = spark.read.csv(base_path + csv_file_path, header=True, inferSchema=True)

# Afficher un aperçu des données
df_static.printSchema()
df_static.show(10)

#formatage de la date en format (YYYY-MM-DD)


# Transfert des données vers hdfs et sauvegarde en parquet
# df_static.write.mode("overwrite").save("hdfs://localhost:9000/user/hadoop/data/climate_data_kaggle")
