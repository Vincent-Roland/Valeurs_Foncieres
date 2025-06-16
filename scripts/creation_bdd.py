import pandas as pd
import mysql.connector

#1. Importer le fichier CSV
data = pd.read_csv("clean_data/ValeursFoncieres_enrichies.csv",dtype={
                       "Code commune": str,
                        "No plan": str,
                        "Code type local": str,
                        "Code departement": str,
                        "Commune": str,
                        })

#3. Remplace les NaN par None
data = data.where(pd.notnull(data), None)

#4. Vérification des valeurs nulles
def clean_row(row):
    return tuple(None if (isinstance(x, float) and pd.isna(x)) else x for x in row)

#5. Connecter la base de données MYSQL au script python
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="azerty",
    database = "valeurs_foncieres"
)

#6.Création du curseur
cursor= conn.cursor()

cursor.execute("DROP TABLE IF EXISTS transactions;")

#7. Création de la table
cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    `Date mutation` DATE,
    `Nature mutation` VARCHAR(50),
    `Valeur fonciere` FLOAT,
    `Code postal` VARCHAR(10),
    `Commune` VARCHAR(100),
    `Code departement` VARCHAR(10),
    `Code commune` VARCHAR(10),
    `Section` VARCHAR(10),
    `No plan` VARCHAR(10),
    `Code type local` VARCHAR(10),
    `Type local` VARCHAR(50),
    `Surface reelle bati` FLOAT,
    `Nombre pieces principales` FLOAT,
    `Surface terrain` FLOAT,
    `prix_m2` FLOAT,
    `CodeINSEE` VARCHAR(10),
    `Population` INT,
    `dens_pop` FLOAT,
    `LIB_DEP` VARCHAR(50)   
);
""")

#8. Insertion des données dans le BDD
batch_size = 1000
rows = [clean_row(row) for _, row in data.iterrows()]

for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    try:
        cursor.executemany("""
            INSERT INTO transactions (
                `Date mutation`, `Nature mutation`, `Valeur fonciere`,
                `Code postal`, `Commune`, `Code departement`, `Code commune`,
                `Section`, `No plan`, `Code type local`, `Type local`,
                `Surface reelle bati`, `Nombre pieces principales`,
                `Surface terrain`, `prix_m2`, `CodeINSEE`, `Population`, `dens_pop`, `LIB_DEP`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        conn.commit()
    except Exception as e:
        print(f"Erreur lors de l'insertion du batch {i // batch_size + 1} : {e}")
        continue

#9 Enregistrer les changement
conn.commit()

#10 Fermer la connexion à la BDD
cursor.close()
conn.close()

print("Données insérées dans MYSQL avec succès")