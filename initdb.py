import string
import random
import uuid
import numpy as np
import psycopg
import requests
from typing import List, Tuple

URL = "https://api.coincap.io/v2/rates/"  # Ensure correct URL

def email(length: int, domains: List[str] = ["example.com", "demo.com", "test.com"]) -> Tuple[str, str]:
    name = "".join(random.choices(string.ascii_lowercase, k=length))
    domain = random.choice(domains)
    email = f"{name}@{domain}"
    return name, email

def user(max_len: int) -> Tuple:
    user_id = str(uuid.uuid4())
    length = np.random.randint(5, max_len + 1)  # Random name length between 5 and max_len
    name, email_ = email(length)
    is_verified = random.choice([True, False])  # Random verification status
    return user_id, name, email_, is_verified

def insert_users(num: int, conn: psycopg.Connection) -> None:
    users = [user(max_len=100) for _ in range(num)]  # Generate user data
    
    insert_query = """
    INSERT INTO users(id, name, email, is_verified)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (email) DO NOTHING;
    """
    
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_query, users)  # Batch insert the users
        conn.commit()  # Commit transaction
        print(f"{num} users inserted successfully.")
    except Exception as e:
        conn.rollback()  # Rollback on error
        print(f"Error inserting users: {e}")

def get_ids(conn: psycopg.Connection) -> List[str]:
    select_query = """
        SELECT id FROM users WHERE is_verified = TRUE;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(select_query)
            ids = cur.fetchall()
            return [id_[0] for id_ in ids]  # Simplified list extraction
    except Exception as e:
        conn.rollback()  # Rollback on error
        print(f"Error retrieving user IDs: {e}")
        return []

def fetch_initial_prices(coins: List[str]) -> List[Tuple[str, float]]:
    prices = []
    for coin in coins:
        url = URL + coin
        try:
            res = requests.get(url)
            
            # Check if the API call was successful
            if res.status_code != 200:
                print(f"Failed to fetch data for coin {coin}. HTTP Status code: {res.status_code}")
                continue

            data = res.json()
            coin_price = float(data['data']['rateUsd'])  # Extract price from the API response
            prices.append((coin, coin_price))  # Append the coin and its price as a tuple

        except requests.exceptions.RequestException as e:
            print(f"API request error for {coin}: {e}")
        except KeyError as e:
            print(f"Unexpected response structure for {coin}: {e}")
    
    return prices  # Return list of tuples (coin_name, price)

def alert(ids: List[str], coins_with_prices: List[Tuple[str, float]]) -> Tuple[str, str, float, bool]:
    try:
        # Select a random coin and its current price
        coin, current_price = random.choice(coins_with_prices)
        id_ = random.choice(ids)  # Select a random user ID
        point = np.random.uniform(low=-0.2, high=0.2)  # Generate a random point change

        # Calculate the alert price by applying the random change to the current price
        alert_price = round(current_price + (current_price * point), 2)
        is_above = point > 0  # Determine if the price is above the current price
        
        return (coin, id_, alert_price, is_above)  # Return the alert tuple
    
    except Exception as e:
        print(f"Error in generating alert: {e}")
        return None

def insert_alerts(num: int, coins: List[str], conn: psycopg.Connection) -> None:
    ids = get_ids(conn)  # Get list of verified user IDs
    if not ids:
        print("No verified users found. Cannot insert alerts.")
        return
    
    coins_with_prices = fetch_initial_prices(coins)  # Fetch prices for the given coins
    if not coins_with_prices:
        print("Failed to fetch initial prices. Cannot generate alerts.")
        return

    alerts = []
    for _ in range(num):
        alert_data = alert(ids, coins_with_prices)
        if alert_data:
            alerts.append(alert_data)  # Collect alert data tuples
    
    if alerts:
        insert_query = """
        INSERT INTO alerts (coin, user_id, price, is_above, created_at, is_triggered)
        VALUES (%s, %s, %s, %s, NOW(), FALSE)
        ON CONFLICT (coin, user_id, price) DO NOTHING;
        """
        try:
            with conn.cursor() as cur:
                cur.executemany(insert_query, alerts)  # Batch insert the alerts
            conn.commit()  # Commit transaction
            print(f"{len(alerts)} alerts inserted successfully.")
        except Exception as e:
            conn.rollback()  # Rollback on error
            print(f"Error inserting alerts: {e}")
    else:
        print("No alerts generated.")

def create_connection(dbname: str, user: str, password: str, host: str, port: int) -> psycopg.Connection:
    try:
        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        print("Connection established successfully.")
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

if __name__ == "__main__":
    # Database configuration
    dbname = "crypto"
    user_ = "user"
    password = "password"
    host = "localhost"  # Or your database host
    port = 5432  # Default PostgreSQL port

    # Create the database connection
    conn = create_connection(dbname, user_, password, host, port)

    if conn:
        try:
            # Step 1: Insert users
            num_users = 1000
            insert_users(num_users, conn)

            # Step 2: Insert alerts
            num_alerts = 20000
            coins = ['bitcoin', 'ethereum', 'litecoin']
            insert_alerts(num_alerts, coins, conn)

            print("Process completed successfully.")
        
        except Exception as e:
            print(f"An error occurred during processing: {e}")

        finally:
            # Ensure the connection is closed properly
            conn.close()
            print("Database connection closed.")