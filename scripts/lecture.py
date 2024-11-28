from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Read Climate Data") \
    .getOrCreate()

# Lire les fichiers Parquet
data = spark.read.parquet("hdfs://localhost:9000/user/hadoop/data/climate_data_kaggle")

# Afficher les premières lignes
data.show()

# Afficher le schéma des données
data.printSchema()