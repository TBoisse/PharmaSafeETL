import requests

# Base URL of BDPM's (french government) API REST
BASE_URL = "https://bdpmgf.vedielaute.fr/api/medicaments/"

# example call : "groupes-generiques" or "specialites", "cis?pretty=true", ex cis : 61266250
def extract_data(get_type : str, compl : str = ""):
    url = BASE_URL + get_type + "/" + compl
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def extract_generiques(max_pages=-1):
    nb_pages = extract_data("groupes-generiques")["pagination"]["pages"]
    ret = []
    for i in range(nb_pages if max_pages == -1 else max_pages):
        ret += extract_data("groupes-generiques", f"?page={i + 1}")["data"]
    return ret

def extract_from_cis(cis):
    try:
        return extract_data("specialites", f"{cis}?pretty=true")
    except:
        print(f"Cis {cis} not found with the API..")
        return {}