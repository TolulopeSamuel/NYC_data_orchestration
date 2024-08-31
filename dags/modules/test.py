# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# from dotenv import load_dotenv
# import os


# # Load environment variables
# load_dotenv()

# def test_connection():
#     try:
#         # Create SQLAlchemy engine
#         engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
#             user=os.getenv('pg_user'),
#             password=os.getenv('pg_password'),
#             host=os.getenv('pg_host'),
#             port=os.getenv('pg_port'),
#             dbname=os.getenv('pg_dbname')
#         ))

#         # Test connection
#         with engine.connect() as connection:
#             result = connection.execute(text("SELECT 1")).fetchone()
#             print("Connection successful. Result:", result)
    
#     except SQLAlchemyError as e:
#         print(f"Connection failed: {e}")

# if __name__ == "__main__":
#     test_connection()






import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# def test_snowflake_connection():
#     try:
#         # Create a Snowflake connection object
#         conn = snowflake.connector.connect(
#             user=os.getenv('sf_user'),
#             password=os.getenv('sf_password'),
#             account=os.getenv('sf_account_identifier'),
#             warehouse=os.getenv('sf_warehouse'),
#             database=os.getenv('sf_database'),
#             schema=os.getenv('sf_schema')
#         )

#         # Create a cursor object and execute a simple query
#         cursor = conn.cursor()
#         cursor.execute("SELECT 1")
#         result = cursor.fetchone()
#         print("Connection successful. Result:", result)
    
#     except Exception as e:
#         print(f"Connection failed: {e}")
    
#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         conn.close()

# if __name__ == "__main__":
#     test_snowflake_connection()


def test_snowflake_connection(engine):
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            print("Connection successful. Result:", result.fetchone())
            
    except Exception as e:
        print("Connection failed:", e)