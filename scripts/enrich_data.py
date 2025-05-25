import pandas as pd

# Chargement des fichiers
data = pd.read_csv("../clean_data/ValeursFoncieres_nettoyees.csv")
pop = pd.read_csv("../data/donnees_communes.csv", sep=";", encoding="utf-8")
pop.columns = pop.columns.str.strip()

# Construction du Code INSEE dans data

#Conversion de la colonne departement en str
data['Code departement'] = data['Code departement'].astype(str).str.zfill(2)
print(data["Code departement"].head(10))


#Conversion de la colonne code commune en str
data['Code commune'] = data['Code commune'].astype(str)

#Suppresion du .0 à la fin
data["Code commune"] = data["Code commune"].str.replace(r'\.0$','',regex=True)

#3 caractères obligatoires
data["Code commune"] = data["Code commune"].str.zfill(3)
print(data["Code commune"].head(10))

data['CodeINSEE'] = data['Code departement'] + data['Code commune']
print(data["CodeINSEE"].head(10))

# Code INSEE dans pop déjà bon, on sécurise quand même
pop['CodeINSEE'] = pop['COM'].astype(str).str.zfill(5)

# Merge
data = data.merge(pop[['CodeINSEE', 'PTOT']], on='CodeINSEE', how='left')

# Renommage
data.rename(columns={'PTOT': 'Population'}, inplace=True)

# Export
data.to_csv("../clean_data/ValeursFoncieres_enrichies.csv", index=False, encoding="utf-8")

