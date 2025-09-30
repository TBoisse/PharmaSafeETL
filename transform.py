import json

def generiques_to_json(generiques_list, output_file="extract_generiques.json"):
    dic = {}
    for gen in generiques_list:
        id_gen = int(gen["id_groupe"])
        cis_gen = int(gen["cis"])
        if id_gen not in dic:
            dic[id_gen] = []
        dic[id_gen].append(cis_gen)
    with open(output_file, "w+") as f:
        json.dump(dic, f, indent=4)
    return True

def get_list_cis(input_file="extract_generiques.json"):
    with open(input_file, "r") as f:
        dic = json.load(f)
    ret = []
    for key in dic:
        ret += dic[key]
    return ret

def cis_data_transformed(cis_data, generique_dic):
    if cis_data == {}:
        return {}
    ret = {}
    ret["cis"] = cis_data["cis"]
    ret["voies_admin"] = cis_data["voies_admin"]
    ret["cip13"] = [pres["cip13"] for pres in cis_data["presentations"]]
    ret["compositions"] = [{"substance" : subs["denomination_substance"], "dosage" : subs["dosage"]} for subs in cis_data["compositions"]]
    ret["generique_id"] = -1
    for id in generique_dic:
        cis = int(ret["cis"])
        if cis in generique_dic[id]:
            ret["generique_id"] = id
            break
    return ret

def medicine_to_json(medicine_list, output_file="extract_medicine.json"):
    dic = {}
    for medicine in medicine_list:
        if medicine == {}:
            continue
        dic[medicine["cis"]] = medicine
    with open(output_file, "w+") as f:
        json.dump(dic, f, indent=4)
    return True