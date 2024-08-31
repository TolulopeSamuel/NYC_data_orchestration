
import pandas as pd

def run_extraction():
    try:
        agency_data = pd.read_csv(r'./dags/raw_files/AgencyMaster.csv') 
        emp_data = pd.read_csv(r'./dags/raw_files/EmpMaster.csv')
        payroll2020_data = pd.read_csv(r'./dags/raw_files/nycpayroll_2020.csv')
        payroll2021_data = pd.read_csv(r'./dags/raw_files/nycpayroll_2021.csv')
        title_data = pd.read_csv(r'./dags/raw_files/TitleMaster.csv')
        
        # agency_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\AgencyMaster.csv')
        # emp_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\EmpMaster.csv')
        # payroll2020_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2020.csv')
        # payroll2021_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2021.csv')
        # title_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\TitleMaster.csv')

        print('All data extracted successfully')
        
        # Save the extracted data to CSVs or other formats if needed, or return them for further processing.
        return agency_data, emp_data, payroll2020_data, payroll2021_data, title_data
    except Exception as e:
        print(f'An error occurred during extraction: {e}')
        return None, None, None, None, None



# def run_extraction():
#     try:
#         agency_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\AgencyMaster.csv')
#         emp_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\EmpMaster.csv')
#         payroll2020_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2020.csv')
#         payroll2021_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2021.csv')
#         title_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\TitleMaster.csv')
#         #nyc_payroll = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nyc_payroll.csv')
        
#         print('All Data extracted successfully')
#     except Exception as e:
#         print(f'An error occured:{e}')
        
# run_extraction()