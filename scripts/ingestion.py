from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.functions import col, trim, lower, when, count


# Créer une session Spark
spark = SparkSession.builder.appName("ClimateData").getOrCreate()

# Définir le chemin de base et le chemin du fichier CSV dans des variables
base_path = "/home/claudia"
csv_file_path = "/Documents/projetData/data/GlobalWeatherRepository.csv"

# Charger les données CSV
df_static = spark.read.csv(base_path + csv_file_path, header=True, inferSchema=True)


# Afficher un aperçu des données
df_static.printSchema()

# Formatages de date et heure

#Formatage de l'heure
df_static = df_static.withColumn("last_updated", to_timestamp("last_updated", "dd-MM-YYYY HH:mm"))

#Formatage de la date
df_static = df_static.withColumn("last_updated", date_format("last_updated", "dd-MM-yyyy HH:mm"))

#df_static.select("last_updated").show(10)


# GESTION DES VALEURS MANQUANTES

#1 Identification des colonnes avec des valeurs manquantes et calcul de pourcentage de données manquantes

# colonnes avec des valeurs manquantes
escaped_columns = [f"`{c}`" for c in df_static.columns]
missing_columns = [ c for c in escaped_columns if df_static.filter(col(c).isNull()).count() > 0]

print("Colonnes avec valeurs manquantes : ", missing_columns)

# calcule de pourcentages des valeurs manquantes
missing_values = df_static.select(
    [(count(when(col(c).isNull(), c)) / count("*")).alias(c + "_missing_percentage") for c in escaped_columns]
)
missing_values.show()

# Stratégie de traitement des valeurs manquantes 
if len(missing_columns) == 0:
    print("Auncune valeur manquante détectée dans le dataset.")
else:
    print("Des valeurs manquantes ont été détecté.\n\n")

print("\nTraitement des valeurs manquantes terminé")

# NORMALISATION DES TEXTES

#identification des colonnes de type texte
text_columns = [field.name for field in df_static.schema.fields if field.dataType.simpleString() == "string"]

#Application de la normalisation de texte
for columns in text_columns:
    df_static = df_static.withColumn(columns, trim(lower(col(columns))))

print("Aperçu des données après normalisation des textes :")
df_static.show(10,truncate=False)

# Test et validation
validation = df_static.select(
    *[
        when(trim(lower(col(c))) != col(c), f"{c} non normalisé").alias(c)
        for c in text_columns
    ]
)
validation.show(truncate=False)

print("Normalisation des textes terminée.")
# Transfert des données vers hdfs
#df_static.write.mode("overwrite").save("hdfs://localhost:9000/user/hadoop/data/climate_data_kaggle")
