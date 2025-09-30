import json
from extract import *
from transform import *

gen_dic = extract_generiques(3)
generiques_to_json(gen_dic)

# with open("extract_generiques.json", "r") as f:
#     gen_dic = json.load(f)

cis_list = get_list_cis()
print(len(cis_list))
cis_raw = [extract_from_cis(cis) for cis in cis_list]
cis_transformed = [cis_data_transformed(raw, gen_dic) for raw in cis_raw]
medicine_to_json(cis_transformed)