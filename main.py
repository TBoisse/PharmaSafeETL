import json
import pandas as pd
from extract import *
from transform import *

PAGES_A_EXTRAIRE = 3
FICHIER_GENERIQUES = "extract_generiques.json"
FICHIER_CSV_FINAL = "medicaments_output.csv"

#---------------------------------------------------------------------------------------
# EXTRACT
#---------------------------------------------------------------------------------------
print("*-* Extracting...")
# fetch generical groups with API
print(f"✅ Extracting generical groups ({PAGES_A_EXTRAIRE} first pages)...")
gen_list_raw = extract_generiques(PAGES_A_EXTRAIRE)
assert gen_list_raw, "*-* Extract error: No generical data extracted."
# store generical groups in json file (extra step to avoid requests to api)
generiques_to_json(gen_list_raw, FICHIER_GENERIQUES)
print(f"✅ Local file '{FICHIER_GENERIQUES}' created.")
# load the json file in a dictionnary
try:
    with open(FICHIER_GENERIQUES, "r") as f:
        generic_dic = json.load(f) # Dict : {"1": [cis1, cis2], ...}
except FileNotFoundError:
    raise f"*-* Extract error: '{FICHIER_GENERIQUES}' not found."
print("✅ Extracting done !")
#---------------------------------------------------------------------------------------
# TRANSFORM
#---------------------------------------------------------------------------------------
print("*-* Transforming...")
cis_list = get_list_cis(FICHIER_GENERIQUES) # get all cis data
assert cis_list, "*-* Transform error: No CIS found in local file."
cis_raw = [extract_from_cis(cis) for cis in cis_list] # extract data related to cis
print("✅ Done extracting caracteristics.")
final_data = transform_data(cis_raw, generic_dic) # flatten cis matrix
print(f"✅ {len(final_data)} medicines found.")
df_final = pd.DataFrame(final_data) # storing database as a csv => cross plateforme
df_final.to_csv(FICHIER_CSV_FINAL, index=False, encoding="utf-8-sig")
print(f"✅ DataFrame save as '{FICHIER_CSV_FINAL}'")
print("✅ Transforming done !")