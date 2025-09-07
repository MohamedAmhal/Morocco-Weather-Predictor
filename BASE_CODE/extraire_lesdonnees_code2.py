#importations des bibliotheques utiles :
from pymongo import MongoClient
import pandas as pd
import numpy as np
from bson.objectid import ObjectId  # Import correct pour ObjectId
from pyspark.sql.functions import rand



# connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Streamingdata"]
collection = db["streaming"]


# projection pour inclure uniquement les colonnes pertinentes
projection = {
    'coord': 1,
    'main.temp': 1,
    'main.feels_like': 1,
    'main.pressure': 1,
    'main.humidity': 1,
    'wind.speed': 1,
    'clouds.all': 1,
    'sys.country': 1,
    'name': 1
}

# fonction pour convertir ObjectId en chaîne de caractères
def convert_oid(data):
    if isinstance(data, ObjectId):
        return str(data)
    return data

# chargement des données dans un DataFrame pandas avec projection
data = list(collection.find({}, projection))
for record in data:
    record['_id'] = convert_oid(record['_id'])

# conversion en DataFrame pandas
df_pd = pd.DataFrame(data)

# extraction des données
def extract_value(dic, key, default=None):
    if isinstance(dic, dict):
        return dic.get(key, default)
    return default

# extraction des données
lon = df_pd['coord'].apply(lambda x: extract_value(x, 'lon'))
lat = df_pd['coord'].apply(lambda x: extract_value(x, 'lat'))
temp = df_pd['main'].apply(lambda x: extract_value(x, 'temp'))
feels_like = df_pd['main'].apply(lambda x: extract_value(x, 'feels_like'))
temp_min = df_pd['main'].apply(lambda x: extract_value(x, 'temp_min', np.min(temp)))
temp_max = df_pd['main'].apply(lambda x: extract_value(x, 'temp_max', np.max(temp)))
pressure = df_pd['main'].apply(lambda x: extract_value(x, 'pressure'))
humidity = df_pd['main'].apply(lambda x: extract_value(x, 'humidity'))
visibility = df_pd['main'].apply(lambda x: extract_value(x, 'visibility', None))
city = df_pd['name']
wind_speed = df_pd['wind'].apply(lambda x: extract_value(x, 'speed'))
wind_deg = df_pd['wind'].apply(lambda x: extract_value(x, 'deg', None))


#création de la DataFrame
df = pd.DataFrame({
    'lon': lon,
    'lat': lat,
    'temp': temp,
    'feels_like': feels_like,
    'temp_min': temp_min,
    'temp_max': temp_max,
    'pressure': pressure,
    'humidity': humidity,
    'visibility': visibility,
    'city': city,
    'wind_speed': wind_speed,
    'wind_deg': wind_deg
})


# convertir les colonnes en type float pour correspondre à DoubleType de Spark
df['lon'] = df['lon'].astype(float)
df['lat'] = df['lat'].astype(float)
df['temp'] = df['temp'].astype(float)
df['feels_like'] = df['feels_like'].astype(float)
df['temp_min'] = df['temp_min'].astype(float)
df['temp_max'] = df['temp_max'].astype(float)
df['pressure'] = df['pressure'].astype(float)
df['humidity'] = df['humidity'].astype(float)
df['visibility'] = df['visibility'].astype(float)
df['wind_speed'] = df['wind_speed'].astype(float)
df['wind_deg'] = df['wind_deg'].astype(float)

print(df.head(10))


#enregistrer les données dans un fichier csv:
df.to_csv(r"C:\Users\hp\Downloads\entrai.csv")


