USE DATABASE NYC_PIPELINE
USE SCHEMA STG

    -- Creating procedure logs table
    CREATE TABLE IF NOT EXISTS "STG".procedure_log(
        run_time TIMESTAMP NOT NULL,
        status TEXT,
        error_message TEXT
    );

    -- Creating aggregate tables
    CREATE TABLE IF NOT EXISTS "PRD".payroll_summary (
    Year INT,
    TotalBasePaid float,
    AverageBasePaid float,
    TotalOTPaid float,
    AverageOTPaid float,
    TotalPaid float,
    AveragePaid float 
    );


    CREATE TABLE IF NOT EXISTS "PRD".agency_summary (
        Agency VARCHAR,
        TotalBasePaid float,
        AverageBasePaid float,
        TotalOTPaid float,
        AverageOTPaid float,
        TotalPaid float,
        AveragePaid float 
    );
    
    
    CREATE TABLE IF NOT EXISTS "PRD".title_summary (
        Title VARCHAR,
        AverageBasePaid float,
        AverageOTPaid float,
        AveragePaid float 
    );



