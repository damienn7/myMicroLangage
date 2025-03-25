
import sys
import re
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase

# Connexion aux bases de données
POSTGRES_CONFIG = {
    'dbname': 'testdb',
    'user': 'user',
    'password': 'password',
    'host': 'localhost',
    'port': 5432
}

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017
}

NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'password'
}

def detect_query_type(query):
    if re.match(r"(?i)select .* from .*", query):
        return 'postgres'
    elif re.match(r"(?i)db\..*\.find\(", query):
        return 'mongo'
    elif re.match(r"(?i)match .* return .*", query):
        return 'neo4j'
    else:
        return 'unknown'

def execute_postgres(query):
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                print(row)
    except Exception as e:
        print(f"Erreur PostgreSQL : {e}")
    finally:
        conn.close()

def execute_mongo(query):
    try:
        client = MongoClient(**MONGO_CONFIG)
        db = client.testdb
        collection_name = re.search(r"db\.(.*?)\.find", query).group(1)
        results = eval(query)
        for doc in results:
            print(doc)
    except Exception as e:
        print(f"Erreur MongoDB : {e}")
    finally:
        client.close()

def execute_neo4j(query):
    try:
        driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
        with driver.session() as session:
            results = session.run(query)
            for record in results:
                print(record)
    except Exception as e:
        print(f"Erreur Neo4j : {e}")
    finally:
        driver.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 main.py "<query>"")
        sys.exit(1)
    
    query = sys.argv[1]
    query_type = detect_query_type(query)
    
    if query_type == 'postgres':
        execute_postgres(query)
    elif query_type == 'mongo':
        execute_mongo(query)
    elif query_type == 'neo4j':
        execute_neo4j(query)
    else:
        print("Type de requête inconnu.")

if __name__ == "__main__":
    main()
