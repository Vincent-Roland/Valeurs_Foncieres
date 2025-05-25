import pandas as pd

colonnes_en_str = ["Code commune", 'Code postal', 'Code departement', 'No plan', 'Section', 'Code type local']

data = pd.read_csv("../data/ValeursFoncieres-2024.txt", 
                   sep="|", 
                   low_memory=False,
                   dtype={col: str for col in colonnes_en_str}
                   )

data_reduit = data.sample(n=400_000, random_state=42)



data_reduit.to_csv(
    "../data/ValeursFoncieres-2024_reduit.csv", 
    sep=";", 
    index=False, 
    )

