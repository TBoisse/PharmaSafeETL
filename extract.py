import requests

# Base URL of BDPM's (french government) API REST
BASE_URL = "https://bdpmgf.vedielaute.fr/api/medicaments/"

# example call : "groupes-generiques" or "specialites", "cis?pretty=true", ex cis : 61266250
def extract_data(get_type : str, compl : str = ""):
    url = BASE_URL + get_type + "/" + compl
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()