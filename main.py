import sys
import re
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
from ply.lex import lex
from ply.yacc import yacc

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

# ==================== ANALYSEUR LEXICAL (LEX) ====================
tokens = (
    'SELECT', 'FROM', 'WHERE',
    'MATCH', 'RETURN',
    'DB', 'FIND',
    'IDENTIFIER', 'COMMA', 'DOT',
    'LPAREN', 'RPAREN',
    'OPERATOR', 'NUMBER', 'STRING'
)

# Définition des tokens
t_SELECT = r'(?i)SELECT'
t_FROM = r'(?i)FROM'
t_WHERE = r'(?i)WHERE'
t_MATCH = r'(?i)MATCH'
t_RETURN = r'(?i)RETURN'
t_DB = r'(?i)db'
t_FIND = r'(?i)find'
t_COMMA = r','
t_DOT = r'\.'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_OPERATOR = r'[=<>]+'

def t_IDENTIFIER(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    t.type = 'IDENTIFIER'
    return t

def t_NUMBER(t):
    r'\d+'
    t.value = int(t.value)
    return t

def t_STRING(t):
    r'\'[^\']*\'|\"[^\"]*\"'
    t.value = t.value[1:-1]  # Supprime les guillemets
    return t

t_ignore = ' \t\n'

def t_error(t):
    print(f"Caractère illégal '{t.value[0]}'")
    t.lexer.skip(1)

# ==================== ANALYSEUR SYNTAXIQUE (YACC) ====================
def p_query(p):
    '''
    query : sql_query
          | mongo_query
          | neo4j_query
    '''
    p[0] = p[1]

def p_sql_query(p):
    'sql_query : SELECT fields FROM IDENTIFIER where_clause'
    p[0] = {'type': 'postgres', 'fields': p[2], 'table': p[4], 'where': p[5]}

def p_mongo_query(p):
    'mongo_query : DB DOT IDENTIFIER DOT FIND LPAREN RPAREN'
    p[0] = {'type': 'mongo', 'collection': p[3]}

def p_neo4j_query(p):
    'neo4j_query : MATCH pattern RETURN fields'
    p[0] = {'type': 'neo4j', 'pattern': p[2], 'fields': p[4]}

def p_fields(p):
    '''
    fields : field
           | fields COMMA field
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]

def p_field(p):
    '''
    field : IDENTIFIER
          | DOT
    '''
    p[0] = p[1]

def p_where_clause(p):
    '''
    where_clause : WHERE condition
                | empty
    '''
    if len(p) == 3:
        p[0] = p[2]
    else:
        p[0] = None

def p_condition(p):
    'condition : IDENTIFIER OPERATOR value'
    p[0] = {'field': p[1], 'operator': p[2], 'value': p[3]}

def p_value(p):
    '''
    value : NUMBER
          | STRING
    '''
    p[0] = p[1]

def p_pattern(p):
    'pattern : IDENTIFIER'
    p[0] = p[1]

def p_empty(p):
    'empty :'
    pass

def p_error(p):
    print("Erreur de syntaxe dans la requête")

# ==================== FONCTIONS D'EXECUTION ====================
def detect_query_type(query):
    lexer = lex()
    parser = yacc()
    
    try:
        result = parser.parse(query, lexer=lexer)
        return result['type'] if result else 'unknown'
    except Exception:
        # Fallback à la détection par regex si l'analyse syntaxique échoue
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
        print("Usage: python3 main.py \"<query>\"")
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