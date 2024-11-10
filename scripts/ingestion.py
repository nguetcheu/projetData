from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format


# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData").getOrCreate()

# Définir le chemin de base et le chemin du fichier CSV dans des variables
base_path = "/home/evrad7"
csv_file_path = "/Documents/projetData/data/GlobalWeatherRepository.csv"

# Charger les données CSV ,
df_static = spark.read.csv(base_path + csv_file_path, header=True, inferSchema=True)

# Afficher un aperçu des données
df_static.printSchema()

#formatage de la data en format (YYYY-MM-DD)
df_static=df_static.withColumn("last_updated",date_format("last_updated","dd-mm-yyyy HH:mm"))

df_static.select("last_updated").show(10)


# Transfert des données vers hdfs
# df_static.write.mode("overwrite").save("hdfs://localhost:9000/user/hadoop/data/climate_data_kaggle")
