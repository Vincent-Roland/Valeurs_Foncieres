#Nettoyage du jeu de données Valeurs Foncieres 2024

import pandas as pd

#Importation de la base de données
data = pd.read_csv("../data/ValeursFoncieres-2024_reduit.csv", sep=";", low_memory=False, encoding="utf-8")

data.columns = data.columns.str.strip()

#Conserver seulement les appartements et les maisons
data = data[(data["Type local"] == "Appartement") | (data["Type local"] == "Maison")]

#Supprimer les colonnes inutiles
cols_to_drop_nan = [c for c in data.columns if data[c].isnull().mean()>0.5]
data.drop(cols_to_drop_nan, axis=1, inplace=True) 


cols_to_drop = ['No disposition', 'No voie', 'Type de voie', 'Code voie', 'Voie', 'Nombre de lots', 'Nature culture']
data.drop(cols_to_drop, axis=1, inplace=True)

# Supprimer doublons
data.drop_duplicates(inplace=True)

#Remplace les virgules par des points
data["Valeur fonciere"] = data["Valeur fonciere"].astype(str).str.replace(",",".",regex=False)
data["Surface reelle bati"] = data["Surface reelle bati"].astype(str).str.replace(",",".", regex=False)

#Conversion en float
data['Valeur fonciere'] = pd.to_numeric(data["Valeur fonciere"], errors="coerce")
data['Surface reelle bati']= pd.to_numeric(data["Surface reelle bati"], errors="coerce")

#remplacement des aberrantes
def detect_outliers(df, feature):
    Q1=df[feature].quantile(0.25)
    Q3=df[feature].quantile(0.75)
    IQR = Q3-Q1
    born_inf = Q1 - 2 * IQR
    born_sup = Q3 + 2 * IQR

    return (df[feature] < born_inf) | (df[feature] > born_sup)

colonne  = "Valeur fonciere"
outliers_mask = detect_outliers(data,colonne)
median = data[colonne].median()
data.loc[outliers_mask, colonne] = median



#Calcul du prix au m²
data["prix_m2"] = data.apply(
      lambda row: row["Valeur fonciere"] / row["Surface reelle bati"]
      if pd.notnull(row["Valeur fonciere"]) and pd.notnull(row["Surface reelle bati"]) and row["Surface reelle bati"] > 0
      else None,
      axis = 1
)


#Sauvegarde du jeu de données
data.to_csv("../clean_data/ValeursFoncieres_nettoyees.csv", index=False, encoding="utf-8")

        
