import pandas as pd
from modules.extract import run_extraction
from datetime import datetime

def run_cleaning():
    try:
        # Extract data by calling the run_extraction function
        agency_data, emp_data, payroll2020_data, payroll2021_data, title_data = run_extraction()

        if payroll2020_data is None or payroll2021_data is None:
            print('Data extraction failed. Transformation aborted.')
            return

        # Rename AgencyName to AgencyID in 2021 payroll data for uniformity
        payroll2021_data.rename(columns={'AgencyName': 'AgencyID'}, inplace=True)

        # Join the 2020 and 2021 NYC payroll datasets together
        nycpayroll = pd.concat([payroll2020_data, payroll2021_data], ignore_index=True)

        # Filter out rows where FiscalYear is less than 2020
        nycpayroll = nycpayroll[nycpayroll['FiscalYear'] >= 2020]

        #changing date datatype for AgencyStartDate column from object to datetime
        date_formats = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"]

        def parse_date(date_string):
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_string, fmt)
                except ValueError:
                    continue
            return pd.NaT 

        nycpayroll['AgencyStartDate'] = nycpayroll['AgencyStartDate'].apply(parse_date)

        # Make a copy of the DataFrame for transformation
        df2 = nycpayroll.copy()

        # Recalculate BaseSalary for rows with PayBasis 'per hour' to derive annual salary
        df2.loc[df2['PayBasis'] == 'per hour', 'BaseSalary'] = df2['BaseSalary'] * 1820

        # Recalculate BaseSalary for rows with PayBasis 'per day' to derive annual salary
        df2.loc[df2['PayBasis'] == 'per day', 'BaseSalary'] = df2['BaseSalary'] * 260

        # Change all the PayBasis values to 'Per Annum'
        df2['PayBasis'] = 'Per Annum'

        # Anonymize FirstName and LastName
        def anonymize_name(name):
            if len(name) > 2:
                return name[:2] + '****'
            return name 

        def anonymize_names(df2):
            df2['FirstName'] = df2['FirstName'].apply(anonymize_name)
            df2['LastName'] = df2['LastName'].apply(anonymize_name)
            return df2

        df2 = anonymize_names(df2)

        # Save the transformed DataFrame as a CSV file
        # Define the full file path
        csv_file_path = r'dags/raw_files/nyc_payroll.csv'
        #csv_file_path = r'C:\Users\User\Desktop\CLOUD\Data_engineering\nyc payroll data orchestration\dags\raw_files\nyc_payroll.csv'
        
        # Save the DataFrame to the specified path
        df2.to_csv(csv_file_path, index=False)        

        print('Data cleaned successfully')
        return csv_file_path
    
    except Exception as e:
        print(f'An error occurred during cleaning: {e}')
        return None
#if __name__ == "__main__":
    #run_cleaning()










# run_extraction()
# agency_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\AgencyMaster.csv')
# emp_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\EmpMaster.csv')
# payroll2020_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2020.csv')
# payroll2021_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2021.csv')
# title_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\TitleMaster.csv')


# def run_transformation():
#     try:
#         # Run the extraction process
#         run_extraction()

#         # Load datasets
#         agency_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\AgencyMaster.csv')
#         emp_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\EmpMaster.csv')
#         payroll2020_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2020.csv')
#         payroll2021_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\nycpayroll_2021.csv')
#         title_data = pd.read_csv(r'NYC PAYROLL DATA INTEGRATION\dataset_nyc\TitleMaster.csv')

#         # renaming agency code on 2021 payroll data fpr uniformity
#         payroll2021_data.rename(columns={'AgencyName': 'AgencyID'}, inplace=True)

#         # #Joining 2020 and 2021 nycpayroll datasets together
#         nycpayroll = pd.concat([payroll2020_data, payroll2021_data], ignore_index=True)

#         # make a copy of the dataframe
#         df2 = nycpayroll.copy()

#         # Recalculate BaseSalary for rows with PayBasis 'per hour' to derive annual salary
#         df2.loc[df2['PayBasis'] == 'per hour', 'BaseSalary'] = df2['BaseSalary'] * 1820

#         # Recalculate BaseSalary for rows with PayBasis 'per Day' to derive annual salary
#         df2.loc[df2['PayBasis'] == 'per day', 'BaseSalary'] = df2['BaseSalary'] * 260

#         # change all the pay basis to pay annum
#         df2['PayBasis'] = 'Per Annum'

#         # saving as csv
#         df2.to_csv('nyc_payroll.csv', index=False)

#         print('Data transformed successfully')
#     except Exception as e:
#         print(f'An error occured:{e}')
        
# run_transformation()

# #changing date datatype for AgencyStartDate column from object to datetime

# date_formats = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"]

# def parse_date(date_string):
#     for fmt in date_formats:
#         try:
#             return datetime.strptime(date_string, fmt)
#         except ValueError:
#             continue
#     return pd.NaT 


# nycpayroll['AgencyStartDate'] = nycpayroll['AgencyStartDate'].apply(parse_date)



# nycpayroll.to_csv('dataset_nyc\nycpayroll.csv', index=False)

# print('Data cleaning and transformation successfully complete')



# # total pay for each year
# # average pay for each year
# # total pay by agency
# # average pay by agency
# # total OT pay by agency
# # average OT pay by agency
# # average pay by tirtle description
