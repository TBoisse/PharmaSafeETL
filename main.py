import json
import pandas as pd
import sys
from extract import *
from transform import *

def extract_data(pages_to_extract, generiques_file):
    """
    Étape E : Extrait toutes les données brutes nécessaires.
    - Groupes génériques (API)
    - Fichier de correspondance (Disque)
    - Spécialités (API)
    Retourne les données brutes prêtes pour la transformation.
    """
    
    print(f"Extraction des groupes génériques ({pages_to_extract} premières pages)...")
    gen_list_raw = extract_generiques(pages_to_extract)
    if not gen_list_raw:
        print("Erreur: Aucune donnée de générique n'a été extraite. Arrêt.")
        return None, None

    generiques_to_json(gen_list_raw, generiques_file)
    print(f"Fichier de correspondance '{generiques_file}' créé.")

    try:
        with open(generiques_file, "r") as f:
            gen_dic = json.load(f) # Dictionnaire {"1": [cis1, cis2], ...}
    except FileNotFoundError:
        print(f"Erreur: '{generiques_file}' non trouvé. Arrêt.")
        return None, None
        
    cis_list = get_list_cis(generiques_file)
    if not cis_list:
        print("Aucun CIS trouvé dans le fichier de correspondance. Arrêt.")
        return None, None

    print(f"Extraction des données pour {len(cis_list)} CIS...")
    cis_raw = [extract_from_cis(cis) for cis in cis_list]
    print("Extraction des spécialités terminée.")
    
    return cis_raw, gen_dic

def transform_data(cis_raw_list, generique_dic):
    """
    Étape T : Transforme les données brutes en une liste plate finale.
    """
    
    # 1. Transformer les données (crée une liste de listes)
    # ex: [ [{'cis':1, 'cip':'A'}], [], [{'cis':2, 'cip':'C'}] ]
    cis_transformed_nested = [cis_data_transformed(raw, generique_dic) for raw in cis_raw_list]
    
    # 2. Aplatir la liste (version "list comprehension")
    cis_transformed_flat = [
        presentation 
        for sublist in cis_transformed_nested 
        for presentation in sublist
    ]
    
    print(f"Transformation terminée. {len(cis_transformed_flat)} présentations trouvées.")
    
    return cis_transformed_flat

def load_data_to_csv(data_list, output_csv_file):
    """
    Étape L : Charge la liste finale dans un DataFrame et le sauvegarde en CSV.
    """
    if not data_list:
        print("\nAucune donnée n'a été transformée, le fichier CSV ne sera pas créé.")
        return False

    try:
        df_final = pd.DataFrame(data_list)
        
        print("\n--- DataFrame final (5 premières lignes) ---")
        print(df_final.head())
        
        df_final.to_csv(output_csv_file, index=False, encoding="utf-8-sig")
        print(f"\nDataFrame sauvegardé avec succès dans '{output_csv_file}'")
        return True
    
    except Exception as e:
        print(f"\nErreur lors de la création du DataFrame ou du CSV : {e}")
        return False


if __name__ == "__main__":
    
    
    PAGES_A_EXTRAIRE = 3
    FICHIER_GENERIQUES = "extract_generiques.json"
    FICHIER_CSV_FINAL = "medicaments_output.csv"

    raw_data, lookup_dic = extract_data(PAGES_A_EXTRAIRE, FICHIER_GENERIQUES)
    
    if raw_data is not None:
        final_data = transform_data(raw_data, lookup_dic)
        
        success = load_data_to_csv(final_data, FICHIER_CSV_FINAL)
        
        if success:
            print("--- Pipeline ETL terminé avec succès. ---")
        else:
            print("--- Pipeline ETL terminé (mais le CSV est vide ou a échoué). ---")
    else:
        print("--- Pipeline ETL terminé avec des erreurs (Étape d'extraction). ---")
        sys.exit(1)