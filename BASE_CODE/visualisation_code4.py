#importer les bibliotheques utilies : 
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st


# logo 
logo = "temp.jpg"  

# chargement du logo
st.sidebar.image(logo, use_column_width=True)

# connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Streamingdata"]
collection = db["pretraitementdesdonnees"]


# extraire les donnees de MongoDB dans un DataFrame Pandas
data = pd.DataFrame(list(collection.find()))


# supprimer la colonne '_id' pour éviter l'erreur de sérialisation
data.drop('_id', axis=1, inplace=True)

# titre de la page
st.title("Visualisation des données :")


#une petit description du projet :
st.sidebar.markdown("# Description de l'application")
st.sidebar.markdown('''Cette application intègre des visualisations de données en continu provenant de Kafka, 
                    traitées en utilisant Spark. Elle offre des aperçus dynamiques des données en temps réel, 
                    permettant de mieux comprendre les tendances et les relations entre les variables telles que la température, 
                    l'humidité et la pression. Les visualisations incluent un histogramme de la température, un diagramme à barres de 
                    l'humidité par ville et un nuage de points de la température par rapport à la pression, 
                    fournissant ainsi une analyse visuelle complète des données en streaming.''')



# description de la première visualisation
st.sidebar.markdown("## Description de la Visualisation 1")
st.sidebar.markdown("Cet histogramme illustre la distribution de la température avec une résolution de 20 intervalles, montrant la fréquence de chaque intervalle ainsi qu'une estimation de la densité de probabilité.")


# visualisation 1 : Histogramme de la température
st.write("### Visualisation 1 : Histogramme de la température")
st.set_option('deprecation.showPyplotGlobalUse', False)
fig, ax = plt.subplots(figsize=(10, 6))
sns.histplot(data['temp'], bins=20, kde=True, ax=ax)
ax.set_xlabel('Température')
ax.set_ylabel('Fréquence')
st.pyplot(fig)


# description de la 2eme visualisation
st.sidebar.markdown("## Description de la Visualisation 2")
st.sidebar.markdown("Un diagramme à barres comparant l'humidité entre différentes villes, avec les villes sur l'axe horizontal et l'humidité sur l'axe vertical.")

# visualisation 2 : Diagramme à barres de l'humidité
st.write("### Visualisation 2 : Diagramme à barres de l'humidité")
st.set_option('deprecation.showPyplotGlobalUse', False)
plt.figure(figsize=(10, 6))
sns.barplot(x='city', y='humidity', data=data)
plt.xlabel('Ville')
plt.ylabel('Humidité')
plt.xticks(rotation=45)
st.pyplot()

# description de la 3eme visualisation
st.sidebar.markdown("## Description de la Visualisation 3")
st.sidebar.markdown("Cette visualisation présente un nuage de points représentant la relation entre la température et la pression atmosphérique. Chaque point représente une observation, avec la température sur l'axe horizontal et la pression sur l'axe vertical.")


# visualisation 3 : Nuage de points température vs pression
st.write("### Visualisation 3 : Nuage de points température vs pression")
st.set_option('deprecation.showPyplotGlobalUse', False)
plt.figure(figsize=(10, 6))
sns.scatterplot(x='temp', y='pressure', data=data)
plt.xlabel('Température')
plt.ylabel('Pression')
st.pyplot()

