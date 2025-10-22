
"""
Data Loading Module

This module handles loading cleaned data into PostgreSQL database:
- Load ... data to ... table
- Load ... data to ... table  
- Verify data was loaded correctly
"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

password = ""
# with open("password/password.txt", "r") as f:
#     password = f.readline().strip()
username = "ahraoui"
# with open("password/username.txt", "r") as f:
#     username = f.readline().strip()

# Database connection configuration
# TODO: Update these values with your actual database credentials
DATABASE_CONFIG = {
    'username': username,
    'password': password,
    'host': 'localhost',
    'port': '5432',
    'database': 'pharmasafe_db'
}

def get_connection_string():
    """Build PostgreSQL connection string"""
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"


def load_to_database(medicaments_df):
    """Load cleaned data into PostgreSQL database"""

    # Connexion PostgreSQL 
    connection_string = get_connection_string()

    try:
        # Créer l'engine SQLAlchemy
        # engine SQLAlchemy = la passerelle entre ton code Python et la base de données. Sans lui, Pandas ne saurait pas où et comment écrire/lire les données
        engine = create_engine(connection_string)

        # Charger la table Médicaments
        if not medicaments_df.empty:
            medicaments_df.to_sql("medicaments", engine, if_exists="replace", index=False)
            print(f"{len(medicaments_df)} médicaments chargés dans la base.")

        print("✅ Données chargées avec succès dans PostgreSQL !")

    except Exception as e:
        print(f"❌ Erreur lors du chargement des données: {e}")


def verify_data():
    """Check that data was loaded correctly"""
    connection_string = get_connection_string()

    try:
        engine = create_engine(connection_string)

        # Compter les médicaments
        medic_count = pd.read_sql("SELECT COUNT(*) FROM medicaments", engine)
        print(f"Médicaments en base: {medic_count.iloc[0,0]}")

        # Montrer un échantillon
        sample_medic = pd.read_sql("SELECT * FROM medicaments LIMIT 3", engine)
        print("Exemple de médicaments :")
        print(sample_medic)

    except Exception as e:
        print(f"❌ Erreur lors de la vérification: {e}")




def run_sample_queries():
    """
    Exécute quelques requêtes d'analyse sur les données chargées
    Permet de valider et d'explorer la base
    """
    print("Lancement des requêtes d'exploration...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Query 1 : Top 5 substances actives les plus fréquentes
        print("\n💊 Top 5 substances les plus présentes dans les médicaments :")
        composition_query = """
        SELECT compositions, COUNT(*) as nb_medicaments
        FROM medicaments
        WHERE compositions IS NOT NULL
        GROUP BY compositions
        ORDER BY nb_medicaments DESC
        LIMIT 5
        """
        compo_results = pd.read_sql(composition_query, engine)
        print(compo_results.to_string(index=False))
        
        # Query 2 : Médicaments génériques vs non génériques A MODIFIER AVEC LES NOMS DES COLONNES MISES PAR TOM 
        print("\n📊 Répartition génériques vs non-génériques :")
        generique_query = """
        SELECT generique_id, COUNT(*) as total
        FROM medicaments
        GROUP BY generique_id
        ORDER BY total DESC
        
        """
        generique_results = pd.read_sql(generique_query, engine)
        print(generique_results.to_string(index=False))
        
        
    except Exception as e:
        print(f"❌ Erreur lors de l’exécution des requêtes: {e}")


def test_database_connection():
    """
    Teste la connexion à la base et la présence des tables
    """
    print("🔌 Test de connexion à la base...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Test simple
        result = pd.read_sql("SELECT 1 as test", engine)
        
        if result.iloc[0]['test'] == 1:
            print("✅ Connexion à la base réussie !")
            
            # Vérifier si nos tables existent
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('medicaments')
            ORDER BY table_name
            """
            tables = pd.read_sql(tables_query, engine)
            
            if len(tables) == 1:
                print("✅ Les tables (medicaments) existent bien")
            else:
                print(f"⚠️  Seulement {len(tables)} table(s) trouvée(s), attendu 1")
                print("💡 Vérifie que la création des tables a été faite correctement")
            
            return True
        else:
            print("❌ Test de connexion échoué")
            return False
            
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        print("💡 Vérifie tes paramètres de connexion PostgreSQL")
        return False


if __name__ == "__main__":
    """Test des fonctions de vérification"""
    print("Testing database loading functions...\n")
    
    if test_database_connection():
        print("\nConnexion OK. Tu peux charger les données !")
        run_sample_queries()
    else:
        print("Corrige la connexion avant de tester les requêtes")
