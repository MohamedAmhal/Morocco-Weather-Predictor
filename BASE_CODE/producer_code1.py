#importation des bibliotheques utiles : 
import requests
import json
import time
from datetime import datetime, timedelta
from pymongo import MongoClient

# le API du plateforme
API_KEY = '13f140b83f752809ac51337d1421aa3e'

# Liste des villes 
cities = villes_maroc = [
    "Casablanca", "Rabat", "Marrakech", "Fès", "Tanger", "Agadir", "Oujda", 
    "Kenitra", "Tétouan", "Salé", "Meknès", "Nador", "Beni Mellal", "El Jadida", 
    "Taza", "Safi", "Mohammedia", "Tiznit",  
    "Bouznika", "Martil" , "El Aïoun", 
    "Youssoufia", "Tafraout"]

# Connexion à MongoDB
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'Streamingdata'
MONGO_COLLECTION = 'streaming'

# Initialisation de la connexion
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Fonction pour extraire les données de l'API
def get_weather(city_name, date):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&dt={date}&appid={API_KEY}"
    response = requests.get(url)
    return response.json()


# Fonction pour générer des dates
def generate_dates():
    current_date = datetime.now()
    # ici on recupere chaque 15 jours après la date actuelle
    for i in range(1, 100, 15):
        yield (current_date - timedelta(days=i)).strftime('%Y-%m-%d')

# Traitement des données
last_fetch_time = {city: None for city in cities}

# Boucle principale
while True:

    # Traitement des données pour chaque ville
    for city in cities:
        for date in generate_dates():
            weather_data = get_weather(city, date)
            weather_data['city'] = city  # ajouter le nom de la ville
            weather_data['date'] = date   # ajouter la date
            print(weather_data)  # printer les données dans le terminal
            collection.insert_one(weather_data)  # Innserer les données dans MongoDB
            time.sleep(1)  # ajouter un delai de 1 seconde entre chaque requête


#fin de l'ingestion ##
