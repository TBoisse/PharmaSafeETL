
import json
import pandas as pd
from extract import *
from transform import *

# Extraction des données brutes des groupes génériques
print("Extraction des groupes génériques (3 premières pages)...")
gen_list_raw = extract_generiques(3)

# Transformation et sauvegarde de la table de correspondance (ID Groupe -> Liste de CIS)
generiques_to_json(gen_list_raw, "extract_generiques.json")
print("Fichier 'extract_generiques.json' créé.")


with open("extract_generiques.json", "r") as f:
    gen_dic = json.load(f) # 'gen_dic' est maintenant un dictionnaire {"1": [cis1, cis2], ...}

cis_list = get_list_cis("extract_generiques.json")
print(f"Extraction des données pour {len(cis_list)} CIS...")

# Extraction des données brutes pour chaque CIS (Appel API)
cis_raw = [extract_from_cis(cis) for cis in cis_list]
print("Extraction des spécialités terminée.")

# Transformation des données
# 'cis_data_transformed' retourne une liste, donc 'cis_transformed_nested'
# sera une LISTE DE LISTES:
cis_transformed_nested = [cis_data_transformed(raw, gen_dic) for raw in cis_raw]

# Résultat : [{'cis': 1, 'cip13': 'A'}, {'cis': 1, 'cip13': 'B'}, ...]
cis_transformed_flat = [
    presentation 
    for sublist in cis_transformed_nested 
    for presentation in sublist
]
print(f"Transformation terminée. {len(cis_transformed_flat)} présentations (couples CIS-CIP) trouvées.")

if cis_transformed_flat:
    df_final = pd.DataFrame(cis_transformed_flat)
    
    print("\n--- DataFrame final (5 premières lignes) ---")
    print(df_final.head())
    
    # sauvegarde du DataFrame en fichier CSV
    try:
        df_final.to_csv("medicaments_output.csv", index=False, encoding="utf-8-sig")
        print("\nDataFrame sauvegardé avec succès dans 'medicaments_output.csv'")
    except Exception as e:
        print(f"\nErreur lors de la sauvegarde du CSV : {e}")
else:
    print("\nAucune donnée n'a été transformée, le DataFrame est vide.")