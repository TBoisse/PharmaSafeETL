import requests

# Base URL de l’API REST BDPM (via data.gouv)
BASE_URL = "https://www.data.gouv.fr/reuses/api-rest-base-de-donnees-publique-des-medicaments/"

def get_specialites(nom=None, page=1, page_size=20):
    """
    Récupère les spécialités pharmaceutiques via l’API BDPM.
    On peut filtrer par nom (wildcards), et paginer.
    """
    params = {
        "page": page,
        "size": page_size,
    }
    if nom:
        # le paramètre exact dépend de la documentation de l’API
        params["q"] = nom  # par exemple, “q” pour query / recherche
    url = BASE_URL + "specialites"
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()

def main():
    # Exemple : chercher les spécialités dont le nom contient "paracétamol"
    nom_recherche = "paracétamol"
    result = get_specialites(nom=nom_recherche, page=1, page_size=10)
    # Afficher quelques résultats
    for spec in result.get("data", []):
        print(f"CIS : {spec.get('cis')}, Nom : {spec.get('denomination')}")
    # Afficher le total de résultats, etc.
    print("Pagination info :", result.get("meta"))

if __name__ == "__main__":
    main()
