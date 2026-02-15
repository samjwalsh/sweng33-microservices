import os
#imports library that lets Python connect to PostgreSQL databases
import psycopg      
from dotenv import load_dotenv

#Loads .env file so python can access environmental variables
load_dotenv()       

#reads DATABASE_URL from .env file and stores it in db_url variable
db_url = os.environ["DATABASE_URL"]


#opens connection to Postgres with db_url
with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:              #cursor is a query runner
        cur.execute("SELECT now();")
        print("DB connected. DB time:", cur.fetchone()[0])
        