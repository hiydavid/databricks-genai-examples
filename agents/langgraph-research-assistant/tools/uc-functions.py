# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog Functions
# MAGIC
# MAGIC User `Serverless` compute

# COMMAND ----------

catalog = "catalog"
schema = "schema"

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`lookup_company_info_by_name`(
        input_name STRING COMMENT 'Name of the company whose info to look up'
    )
    RETURNS STRING
    COMMENT "Returns metadata about a particular company, given the company's name, including its TICKER and SEDOL. The
    company TICKER or SEDOL can be used for other queries."
    RETURN SELECT CONCAT(
        'Company Name: ', name, ', ',
        'Company Ticker: ', ticker, ', ',
        'Company Sedol: ', sedol
    )
    FROM `{catalog}`.`{schema}`.`security_reference_mapping`
    WHERE lower(name) like lower(concat('%', input_name, '%'))
    LIMIT 1;          
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`lookup_company_info_by_ticker`(
        input_ticker STRING COMMENT 'Ticker of the company whose info to look up'
    )
    RETURNS STRING
    COMMENT "Returns metadata about a particular company, given the company's name, including its TICKER and SEDOL. The
    company TICKER or SEDOL can be used for other queries."
    RETURN SELECT CONCAT(
        'Company Name: ', name, ', ',
        'Company Ticker: ', ticker, ', ',
        'Company Sedol: ', sedol
    )
    FROM `{catalog}`.`{schema}`.`security_reference_mapping`
    WHERE lower(ticker) like lower(concat('%', input_ticker, '%'))
    LIMIT 1;          
    """
)

# COMMAND ----------
