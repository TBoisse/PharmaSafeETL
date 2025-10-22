# Fichier: main.py (Version propre et structurée)

import json
import pandas as pd
import sys
from extract import *
from transform import *

def run_etl_pipeline(pages_to_extract, generiques_file, output_csv_file):
    """
    Exécute le pipeline ETL complet :
    1. Extrait les groupes génériques.
    2. Crée un fichier de correspondance.
    3. Extrait les spécialités (CIS) à partir de la liste.
    4. Transforme et aplatit les données (CIS-CIP).
    5. Sauvegarde le résultat dans un DataFrame et un fichier CSV.
    """
    
    # Génériques
    print(f"Extraction des groupes génériques ({pages_to_extract} premières pages)...")
    gen_list_raw = extract_generiques(pages_to_extract)
    if not gen_list_raw:
        print("Erreur: Aucune donnée de générique n'a été extraite. Arrêt.")
        return False

    generiques_to_json(gen_list_raw, generiques_file)
    print(f"Fichier de correspondance '{generiques_file}' créé.")

    # Lecture de la correspondance
    try:
        with open(generiques_file, "r") as f:
            gen_dic = json.load(f) 
    except FileNotFoundError:
        print(f"Erreur critique: '{generiques_file}' non trouvé. Arrêt.")
        return False
        
    cis_list = get_list_cis(generiques_file)
    if not cis_list:
        print("Aucun CIS trouvé dans le fichier de correspondance. Arrêt.")
        return False

    print(f"Extraction des données pour {len(cis_list)} CIS...")

    # Spécialités
    cis_raw = [extract_from_cis(cis) for cis in cis_list]
    print("Extraction des spécialités terminée.")

    # Aplatissement
    cis_transformed_nested = [cis_data_transformed(raw, gen_dic) for raw in cis_raw]

    # Aplatissement en une liste simple de dictionnaires
    cis_transformed_flat = [
        presentation 
        for sublist in cis_transformed_nested 
        for presentation in sublist
    ]

    if not cis_transformed_flat:
        print("\nAucune donnée n'a été transformée. Le fichier final sera vide.")
        return True # Ce n'est pas une erreur, juste pas de données

    print(f"Transformation terminée. {len(cis_transformed_flat)} présentations (couples CIS-CIP) trouvées.")

    # Load to DataFrame & CSV
    try:
        df_final = pd.DataFrame(cis_transformed_flat)
        
        print("\n--- DataFrame final (5 premières lignes) ---")
        print(df_final.head())
        
        # Sauvegarde du DataFrame en fichier CSV
        df_final.to_csv(output_csv_file, index=False, encoding="utf-8-sig")
        print(f"\nDataFrame sauvegardé avec succès dans '{output_csv_file}'")
        return True # Succès
        
    except Exception as e:
        print(f"\nErreur lors de la sauvegarde du CSV : {e}")
        return False # Échec


if __name__ == "__main__":
    
    PAGES_A_EXTRAIRE = 3
    FICHIER_GENERIQUES = "extract_generiques.json"
    FICHIER_CSV_FINAL = "medicaments_output.csv"
    
    print("--- Démarrage du pipeline ETL ---")
  
    success = run_etl_pipeline(
        pages_to_extract=PAGES_A_EXTRAIRE,
        generiques_file=FICHIER_GENERIQUES,
        output_csv_file=FICHIER_CSV_FINAL
    )
    
    if success:
        print("--- Pipeline ETL terminé avec succès. ---")
    else:
        print("--- Pipeline ETL terminé avec des erreurs. ---")
        sys.exit(1)