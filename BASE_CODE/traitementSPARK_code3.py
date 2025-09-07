#importation des bibliotheques utiles : 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, round, mean, stddev, max
from pymongo import MongoClient
import pandas as pd

# creation d'une session Spark et initialisation de spart session
spark = SparkSession.builder \
    .appName("MonApplicationSpark") \
    .getOrCreate()

# affichez les informations sur la session Spark
print("Session Spark initialisée avec succès!")
print("Version de Spark:", spark.version)


# Lire le fichier CSV en tant que DataFrame
df = spark.read.csv(r"C:\Users\hp\Downloads\entrai.csv", header=True, inferSchema=True)


# Afficher le schéma du DataFrame
df.printSchema()


# Afficher les premières lignes du DataFrame
df.show()


# compter le nombre de lignes dans le DataFrame
row_count = df.count()
print("Nombre de lignes dans le DataFrame:", row_count)


#afficher les valeurs manquantes pour chaque colonne
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()


#dans l'affichage de la colonne 'visibility' il y a 100 valeurs manquantes donc 100% de la colonne 'visibility' est vide
#suppression de la colonne 'visibility'
df = df.drop('visibility')

#dropoper les valeurs manquantes de chaue colonne
#df = df.dropna()    on active ce code si on a seulement des valeurs manquantes dans les autres colonnes 


#aussi meme chose dans l'affichage de la colonne 'wind_deg' il y a 100 valeurs manquantes donc 100% de la colonne 'visibility' est vide
#suppression de la colonne 'wind_deg'
df = df.drop('wind_deg')


#affichage des donnees apres la suppression
df.show()




################################################################statistique de chaque columns ###########################################################


# recuperer la liste des noms de colonnes
column_names = df.columns


# afficher les statistiques de chaque colonne individuellement sauf la colonne city categorique
for column in column_names:
    if column != 'city' and column != '_c0':
        print(f"Statistiques pour la colonne '{column}':")
        df.select(column).describe().show()




#####################################################################NORMALISATION DES DONNEES##############################################


# Convertir température de Kelvin en degrés Celsius
df = df.withColumn("temp", round((col("temp") - 273.15), 2).cast("double"))
df = df.withColumn("temp_min", round((col("temp_min") - 273.15), 2).cast("double"))
df = df.withColumn("temp_max", round((col("temp_max") - 273.15), 2).cast("double"))
df = df.withColumn("feels_like", round((col("feels_like") - 273.15), 2).cast("double"))


#afficher les donnees apres la normalisation1
df.show()



#normaliser la colonne pressure pour rendre le calcul plus precis (Zscore normalisation)

# Calculer la moyenne et l'écart-type de la colonne 'pressure'
pressure_stats = df.select(mean(col("pressure")).alias("mean_pressure"),
                           stddev(col("pressure")).alias("stddev_pressure")).collect()

mean_pressure = pressure_stats[0]["mean_pressure"]
stddev_pressure = pressure_stats[0]["stddev_pressure"]

# Normaliser la colonne 'pressure' en utilisant la formule Z-score
df = df.withColumn("pressure", round((col("pressure") - mean_pressure) / stddev_pressure, 2))



#afficher les donnees apres la normalisation1
df.show()



###########################################################ENREGISTREMENT DES DONNEES####################################################


# connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Streamingdata"]
collection = db["pretraitementdesdonnees"]


# convertir le DataFrame Spark en Pandas DataFrame
df_pandas = df.toPandas()

# insérer les données dans MongoDB
collection.insert_many(df_pandas.to_dict("records"))



##########################################################################MACHING LEARNING SPARKMLIB#############################################

#importation des bibliotheques utiles : 
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor  #les modeles de maching learning
from pyspark.ml.evaluation import RegressionEvaluator


# assemblage des features
feature_columns = ['lon', 'lat', 'feels_like', 'pressure', 'humidity', 'wind_speed']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df = assembler.transform(df)


# normalisation des features (la normalisation automatiques)
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)


# definir la colonne de label (la température à prédire)
df = df.withColumn("label", col("temp"))


# separer les données en ensembles d'entraînement et de test  
train_data, test_data = df.randomSplit([0.8, 0.2], seed=1234)   # comme test_split dans skitkearn pour diviser les donnes en entrainement et test



# initialiser les modèles
lr = LinearRegression(featuresCol="scaledFeatures", labelCol="label")
rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol="label")
gbt = GBTRegressor(featuresCol="scaledFeatures", labelCol="label")



# entraîner les modèles
lr_model = lr.fit(train_data)
rf_model = rf.fit(train_data)
gbt_model = gbt.fit(train_data)

# faire des prédictions
lr_predictions = lr_model.transform(test_data)
rf_predictions = rf_model.transform(test_data)
gbt_predictions = gbt_model.transform(test_data)

# evaluer les modèles
evaluator_rmse = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
evaluator_mae = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="mae")
evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="r2")

# les parametres d'evaluation du modele lenear regression
lr_rmse = evaluator_rmse.evaluate(lr_predictions)
lr_mae = evaluator_mae.evaluate(lr_predictions)
lr_r2 = evaluator_r2.evaluate(lr_predictions)

# les parametres d'evaluation du modele random forst
rf_rmse = evaluator_rmse.evaluate(rf_predictions)
rf_mae = evaluator_mae.evaluate(rf_predictions)
rf_r2 = evaluator_r2.evaluate(rf_predictions)

# les parametres d'evaluation du modele gradient boosting
gbt_rmse = evaluator_rmse.evaluate(gbt_predictions)
gbt_mae = evaluator_mae.evaluate(gbt_predictions)
gbt_r2 = evaluator_r2.evaluate(gbt_predictions)


#afichage 1 
print("Linear Regression Metrics:")
print(f"RMSE: {lr_rmse}")
print(f"MAE: {lr_mae}")
print(f"R2: {lr_r2}")

#afichage 2
print("Random Forest Metrics:")
print(f"RMSE: {rf_rmse}")
print(f"MAE: {rf_mae}")
print(f"R2: {rf_r2}")

#afichage 3 
print("Gradient Boosting Metrics:")
print(f"RMSE: {gbt_rmse}")
print(f"MAE: {gbt_mae}")
print(f"R2: {gbt_r2}")


# Afficher les prédictions pour le modele de regression lineare
lr_predictions.select("label", "prediction").show()


#arret du spark
spark.stop()



## fin


