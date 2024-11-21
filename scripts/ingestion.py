import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, trim

# Configurer le logging
logging.basicConfig(level=logging.INFO)

# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Définir le chemin de base et le chemin du fichier CSV dans des variables
base_path = "/home/nguetcheu"
csv_file_path = "/Documents/projetData/data/climate_change_impact_on_agriculture_2024.csv"

# Charger les données CSV
df_static = spark.read.csv(base_path + csv_file_path, header=True, inferSchema=True)
logging.info("Fichier CSV chargé avec succès.")

# Afficher un aperçu des données
df_static.printSchema()

# Renommer les colonnes problématiques
renamed_columns = [col_name.replace('.', '_') for col_name in df_static.columns]
df_static = df_static.toDF(*renamed_columns)
logging.info(f"Colonnes renommées : {renamed_columns}")

# Identification des colonnes numériques et textuelles
numeric_cols = [field.name for field in df_static.schema.fields if field.dataType.simpleString() in ["int", "double", "float"]]
text_cols = [field.name for field in df_static.schema.fields if field.dataType.simpleString() == "string"]

# Remplir les valeurs manquantes
df_filled = df_static.fillna({col: 0 for col in numeric_cols}).fillna({col: "vide" for col in text_cols})
logging.info("Valeurs manquantes remplies.")

# Normaliser les colonnes de texte
df_normalized = df_filled.select(
    *[col(c).alias(c) for c in numeric_cols],  # Garder les colonnes numériques
    *[lower(trim(col(c))).alias(c) for c in text_cols]  # Normaliser les colonnes textuelles
)
logging.info("Colonnes textuelles normalisées.")

# Aperçu des données nettoyées
logging.info("Aperçu des données normalisées :")
df_normalized.show(5)

# Sauvegarder les données au format Parquet sur HDFS
hdfs_path = "hdfs://localhost:9000/user/hadoop/data/climate_data_kaggle"
#df_normalized.write.mode("overwrite").parquet(hdfs_path)
logging.info(f"Données sauvegardées sur HDFS à l'emplacement : {hdfs_path}")