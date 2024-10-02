import string
import random
import uuid
import numpy as np
import psycopg
from typing import List, Tuple

def email(length: int, domains: List[str] = ["example.com", "demo.com", "test.com"]) -> Tuple[str, str]:
    name = "".join(random.choices(string.ascii_lowercase, k=length))
    domain = random.choice(domains)
    email = f"{name}@{domain}"
    return name, email

def user(max_len: int) -> Tuple:
    user_id = str(uuid.uuid4())
    length = np.random.randint(5, max_len + 1)  # Generating a random name length between 5 and max_len
    name, email_ = email(length)
    is_verified = random.choice([True, False])  # Randomly choosing verification status
    return user_id, name, email_, is_verified

def insert_users(num: int, conn: psycopg.Connection) -> None:
    users = []
    for _ in range(num):
        users.append(user(max_len=100))
    
    insert_query = """
    INSERT INTO users(id, name, email, is_verified)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (email) DO NOTHING;
    """
    
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_query, users)  # Batch insert the users
        conn.commit()  # Commit the transaction
        print(f"{num} users inserted successfully.")
    except Exception as e:
        conn.rollback()  # Rollback in case of error
        print(f"Error inserting users: {e}")

def create_connection(dbname: str, user: str, password: str, host: str, port: int) -> psycopg.Connection:
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        print("Connection established successfully.")
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

# Main logic to establish connection and insert users
if __name__ == "__main__":
    # Replace with your actual database credentials
    dbname = "crypto"
    user_ = "user"
    password = "password"
    host = "localhost"  # Or your database host
    port = 5432  # Default PostgreSQL port

    # Create the database connection
    conn = create_connection(dbname, user_, password, host, port)
    
    if conn:
        # Insert 1,000,000 users into the database
        insert_users(1000000, conn)
        
        # Close the connection after completing the insertion
        conn.close()
        print("Connection closed.")