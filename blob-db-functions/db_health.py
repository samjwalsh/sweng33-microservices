import os
#imports library that lets Python connect to PostgreSQL databases
import psycopg      
from dotenv import load_dotenv

#Loads .env file so python can access environmental variables
load_dotenv()       



def retrieve_oldest_queued_job():
    """Returns the id and blob location of the oldest queued job, and sets its status to processing"""
    
    db_url = os.environ["DATABASE_URL"]

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Pick + lock the oldest queued job so two workers don't claim the same one
            cur.execute("""
                SELECT id, blob
                FROM "pg-drizzle_videos"
                WHERE status = 'queued'
                ORDER BY created_at ASC             
                FOR UPDATE SKIP LOCKED
                LIMIT 1;
            """)
            row = cur.fetchone()    #fetch the result of query

            if row is None:
                return None

            job_id, blob = row

            #now update the status of job to 'processing'
            cur.execute("""
                UPDATE "pg-drizzle_videos"
                SET status = 'processing'
                WHERE id = %s;
            """, (job_id,))

            conn.commit()
            return job_id, blob

   


def update_job_status(job_id: int, completed_blob_location: str):
    """Updates the status of a job to done and sets the completed_blob field to the location of the new video file"""
    
    
    db_url = os.environ["DATABASE_URL"]

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM "pg-drizzle_videos"
                WHERE id = %s;
            """, (job_id,))
            row = cur.fetchone()

            if row is None:
                return False

            #updates status to done and updates value of 'completed_blob' to the new location
            cur.execute("""
                UPDATE "pg-drizzle_videos"
                SET status = 'done',
                    completed_blob = %s
                WHERE id = %s;
            """, (completed_blob_location, job_id))

            conn.commit()
            return True
            
    

if __name__ == "__main__":
    #reads DATABASE_URL from .env file and stores it in db_url variable
    db_url = os.environ["DATABASE_URL"]

    #opens connection to Postgres with db_url
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:              #cursor is a query runner
            cur.execute("SELECT now();")
            print("DB connected. DB time:", cur.fetchone()[0])




#TESTS 

#test to see if retrieve_oldest_queued_job() works
if __name__ == "__main__":
    result = retrieve_oldest_queued_job()
    print("claim_oldest_queued_job() ->", result)


#test to see if status has actually changed
with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, status, created_at, blob
            FROM "pg-drizzle_videos"
            ORDER BY created_at ASC
            LIMIT 5;
        """)
        print(cur.fetchall())