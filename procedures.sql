-- Creating procedure and Inserting into tables

CREATE OR REPLACE PROCEDURE "STG".agg_nycdata()
LANGUAGE plpgsql
AS $$
DECLARE
	v_runtime TIMESTAMP;
	v_status TEXT;
	v_error_msg TEXT;
BEGIN
	v_runtime := NOW();
	v_status := 'SUCCESS';
	v_error_msg := NULL;

-- Populating the aggregate tables
	INSERT INTO "PRD".payroll_summary
    SELECT 
    FiscalYear AS "Year",
    SUM(RegularGrossPaid ) AS TotalBasePaid,
    AVG(RegularGrossPaid ) AS AverageBasePaid,
    SUM(TotalOTPaid) AS TotalOTPaid,
    AVG(TotalOTPaid) AS AverageOTPaid,
    SUM(RegularGrossPaid + TotalOTPaid + TotalOtherPay) AS TotalPaid,
    AVG(RegularGrossPaid + TotalOTPaid + TotalOtherPay) AS AveragePaid
    FROM "STG".nyc_payroll
    GROUP BY FiscalYear;


-- agency aggregate table populating

	INSERT INTO "PRD".agency_summary
    SELECT 
    AgencyName AS Agency,
    SUM(RegularGrossPaid ) AS TotalBasePaid,
    AVG(RegularGrossPaid ) AS AverageBasePaid,
    SUM(TotalOTPaid) AS TotalOTPaid,
    AVG(TotalOTPaid) AS AverageOTPaid,
    SUM(RegularGrossPaid + TotalOTPaid + TotalOtherPay) AS TotalPaid,
    AVG(RegularGrossPaid + TotalOTPaid + TotalOtherPay) AS AveragePaid
    FROM "STG".nyc_payroll
    GROUP BY AgencyName;


-- title aggregate table populating

	INSERT INTO "PRD".title_summary
    SELECT 
    TitleDescription AS Title,
    AVG(RegularGrossPaid ) AS AverageBasePaid,
    AVG(TotalOTPaid) AS AverageOTPaid,
    AVG(RegularGrossPaid + TotalOTPaid + TotalOtherPay) AS AveragePaid
    FROM "STG".nyc_payroll
    GROUP BY TitleDescription;

	
	--log the outcome
	INSERT INTO "STG".procedure_log(run_time, status, error_message)
	VALUES (v_runtime, v_status, v_error_msg);
	
EXCEPTION
	WHEN OTHERS THEN
		v_status := 'FAILED';
		v_error_msg := SQLERRM;
		
		INSERT INTO "STG".procedure_log(run_time, status, error_message)
		VALUES (v_runtime, v_status, v_error_msg);
END;
$$;