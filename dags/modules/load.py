import pandas as pd
from modules.helpers import get_snowflake_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from modules.clean import run_cleaning
    
engine = get_snowflake_engine()


def load_csv_to_snowflake(table_name, engine, schema):
    try:
        '''
        load data from a csv file to datatframe to snowflake database table
    
        parameters:
        - table_name(str): snowflake database table
        _ engine(sqlalchemy.engine): sqlalchemy engine object
        _ schema(str): a snowflake DB schema
        '''
        table_name = 'nyc_payroll'
        schema = 'STG'
        csv_file_path = run_cleaning()
        # csv_file_path =r'dags\raw_files\nyc_payroll.csv'

        if csv_file_path is None:
            print("run_cleaning() did not return a valid file path. Exiting the load process.")
            return
        
        #read csv to pandas and to sql
        df = pd.read_csv(csv_file_path)
        df.to_sql('nyc_payroll', con=engine, if_exists='replace', index=False, schema=schema)

        print(f'{len(df)}rows loaded to staging successfully')

    except Exception as e:
        print(f'An error occurred during loading to stg: {e}')
        return None


def exec_procedure(engine):
    try:
        # Execute the procedure
        session = sessionmaker(bind=engine)
        session = session()
        session.execute('CALL "STG".agg_nycdata()')
        session.commit()
        
        print('Stored procedure executed successfully')

        print('pipeline executed successfully')

    except Exception as e:
        print(f'An error occurred during loading to production: {e}')
        return None