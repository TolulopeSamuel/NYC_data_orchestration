import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(override=True)

def get_snowflake_engine():
    """
    Constructs a SQLAlchemy engine object for Snowflake DB from .env file.

    Parameters: None

    Returns:
    - SQLAlchemy engine (sqlalchemy.engine.Engine)
    """

    engine = create_engine(
        'snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse_name={warehouse}'.format(
            user=os.getenv('sf_user'),
            password=os.getenv('sf_password'),
            account_identifier=os.getenv('sf_account_identifier'),
            database=os.getenv('sf_database'),
            schema=os.getenv('sf_schema'),
            warehouse=os.getenv('sf_warehouse')
        ) #,
        # connect_args={'insecure_mode': True}
    )
    
    return engine



# def get_postgres_engine():
    
#     """
#     Constructs a SQLalchemy engine object for Postgres DB from .env file
    
#     Paremeters: None
    
#     Returns:
#     -sqlachemy engine (sqlalchemy.engine.Engine)
#     """

   
#     engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
#                             user = os.getenv('pg_user'),
#                             password = os.getenv('pg_password'),
#                             host = os.getenv('pg_host'),
#                             port = os.getenv('pg_port'),
#                             dbname = os.getenv('pg_dbname')
#                                  )
#                         )
    
#     return engine
