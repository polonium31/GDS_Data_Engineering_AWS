import mysql.connector
import random
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connection configuration
config = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_NAME')
}

def generate_dummy_data():
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        print("Starting data simulation... Press Ctrl+C to stop.")

        categories = ['Electronics', 'Home Decor', 'Apparel', 'Books', 'Fitness']
        product_names = ['UltraTab', 'SmartLight', 'EcoBottle', 'ZenChair', 'ProHeads']

        while True:
            action = random.choice(['INSERT', 'INSERT', 'INSERT', 'INSERT', 'UPDATE'])
            
            # Get the current highest ID to handle increments
            cursor.execute("SELECT MAX(id) FROM product")
            max_id = cursor.fetchone()[0] or 0

            if action == 'INSERT':
                new_id = max_id + 1
                name = f"{random.choice(product_names)} {new_id}"
                cat = random.choice(categories)
                price = round(random.uniform(10.0, 500.0), 2)
                
                sql = "INSERT INTO product (id, name, category, price, last_updated) VALUES (%s, %s, %s, %s, %s)"
                val = (new_id, name, cat, price, datetime.now())
                cursor.execute(sql, val)
                print(f"[INSERT] Added Product ID {new_id}")

            elif action == 'UPDATE' and max_id > 0:
                target_id = random.randint(1, max_id)
                new_price = round(random.uniform(10.0, 500.0), 2)
                
                sql = "UPDATE product SET price = %s, last_updated = %s WHERE id = %s"
                val = (new_price, datetime.now(), target_id)
                cursor.execute(sql, val)
                print(f"[UPDATE] Modified Price for Product ID {target_id}")

            conn.commit()
            time.sleep(2)

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    generate_dummy_data()