from modules.helpers import get_snowflake_engine #, get_postgres_engine
from modules.extract import run_extraction
from modules.transform import run_transformation
from modules.load import load_csv_to_postgres
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, inspect

engine = get_snowflake_engine()

def main():
    
    """
    Main function to run the data pipeline modules
    1. extract data
    2. normalise data
    3. load data to postgress 
    parameters: None
    returns : None
    """
    
    # extract the data
    run_extraction()

    # transform the data
    run_transformation()

    # load the data
    load_csv_to_postgres('./raw_files/nyc_payroll.csv', "nyc_payroll", engine, 'STG')
                      

    # Check if the table exists
    inspector = inspect(engine)
    if 'nyc_payroll' not in inspector.get_table_names(schema='STG'):
        print("Table 'nyc_payroll' does not exist in schema 'STG'")
        return

    # Execute the procedure
    session = sessionmaker(bind=engine)
    session = session()
    session.execute(text('CALL "STG".agg_nycdata()'))
    session.commit()
    
    print('pipeline executed successfully')
    
    print('Stored procedure executed successfully')
    

if __name__== '__main__':
    main()












# engine = get_postgres_engine()

# def main():
    
#     """
#     Main function to run the data pipeline modules
#     1. extract data
#     2. normalise data
#     3. load data to postgress 
#     parameters: None
#     returns : None
#     """
    
#     # extract the data
#     run_extraction()

#     # transform the data
#     run_transformation()

#     # load the data
#     load_csv_to_postgres('nyc_payroll.csv', "nyc_payroll", engine, 'STG')

#     # Check if the table exists
#     inspector = inspect(engine)
#     if 'nyc_payroll' not in inspector.get_table_names(schema='STG'):
#         print("Table 'nyc_payroll' does not exist in schema 'STG'")
#         return


#     session = sessionmaker(bind=engine)
#     session = session()
#     session.execute(text('CALL "STG".agg_nycdata()'))
#     session.commit()
    
#     print('pipeline executed successfully')
    
#     print('Stored procedure executed successfully')
    

# if __name__== '__main__':
#     main()