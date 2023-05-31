import os
from dotenv import load_dotenv
import psycopg2


def db_connect():
    # Load environment variables from .env file
    load_dotenv()

    # Get the database connection parameters from environment variables
    db_params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
    }

    # Connect to the database
    conn = psycopg2.connect(**db_params)

    return conn
