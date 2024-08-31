This project integrates the payroll data for the city of New York (NYC) to help with the analysis of its financial resource allocation and also to help with transparency and data accessibility to the public. My task was to create a data pipeline that is dynamic, automated, scalable, and monitored for efficient operation.

The objectivec of the project are as follows:
1. Design a data warehouse for NYC payroll data
2. Develop a scalable and automated ETL Pipeline using Apache Airflow to load the payroll data NYC data warehouse
3. Develop aggregate table(s) in the data warehouse for easy analysis of the key business questions
4. Create a public user with limited privileges to enable public access to the NYC data warehouse
5. Integrate version control and maintain a cloud-hosted repository of your codebase to facilitate collaboration

Tools Used:
Docker
Snowflake
VisualStudio
Python, SQL, Pandas
ApacheAirflow
SQLAlchemy

Structure of the Warehouse:

Warehouse Name: nyc_pipeline
Schemas
STG
  Tables: nyc_payroll
  Procedure: agg_nycdata
PRD
  Tables: agency_summary, payroll_summary. title_summary

Steps:

Create python modules with functions for the data extraction, cleaning and loading phases
Create DAG scripts with task for each function in the modules
Mount the folder on a Docker container built with apache-airflow installed to enable running the DAG scripts with airflow scheduler
Using SQLAlchemy, create an engine to connect to Snowflake to load the data and execute the procedure when DAG script is run.
Schedule tasks to run at interval with incremental loading

Aggregations: Total and Average Base Salary Paid, OT Paid, and Gross Paid aggregated by Year
	        Total and Average Base Salary Paid, OT Paid, and Gross Paid aggregated by Agency
	         Average Base Salary Paid, OT Paid, and Gross Paid aggregated by Employee Title

