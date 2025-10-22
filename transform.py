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
    """
    Transforme les données brutes d'un CIS en une LISTE de dictionnaires.
    Chaque dictionnaire représente une présentation (un couple CIS-CIP13).
    """
    # Si l'API n'a rien trouvé (ex: 404), cis_data est {}
    if cis_data == {}:
        return []  # Retourner une liste vide

    presentations_list = []
    
    cis = cis_data.get("cis")
    voies_admin = cis_data.get("voies_admin")
    compositions = [
        {"substance": subs.get("denomination_substance"), "dosage": subs.get("dosage")} 
        for subs in cis_data.get("compositions", [])
    ]

    # Trouver l'ID générique (une seule fois par CIS)
    generique_id = -1
    if cis:
        for id_gen in generique_dic:
            # On vérifie si le CIS (entier) est dans la liste des valeurs du dictionnaire
            if int(cis) in generique_dic[id_gen]:
                generique_id = id_gen # id_gen est la clé 

    #  Boucle sur chaque présentation pour créer un dictionnaire séparé 
    for pres in cis_data.get("presentations", []):
        presentation_data = {}
        
        presentation_data["cip13"] = pres.get("cip13")
        presentation_data["libelle_presentation"] = pres.get("libelle")
        presentation_data["prix_medicament"] = pres.get("prix_medicament")

        presentation_data["cis"] = cis
        presentation_data["voies_admin"] = voies_admin
        presentation_data["compositions"] = compositions
        presentation_data["generique_id"] = generique_id
        
        presentations_list.append(presentation_data)

    return presentations_list 

def medicine_to_json(medicine_list, output_file="extract_medicine.json"):
    """
    Sauvegarde directement la liste "plate" de présentations dans un fichier JSON.
    """
    with open(output_file, "w+") as f:
        json.dump(medicine_list, f, indent=4)
    return True