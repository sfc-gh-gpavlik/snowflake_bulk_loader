# Snowflake Bulk Loader
Snowflake Bulk Loader loads thousands to millions of staged files into Snowflake tables.

Note: This software is a community project released under the Apache 2.0 License, not official Snowflake software.

This project loads files from Snowflake internal or external stages to one or more Snowflake tables in an order you specify. It is completely contained in Snowflake requiring no external dependencies. It support massively parallel execution using multiple Snowflake warehouses. A control table tracks ingestion status and if necessary the procedure retries failed copy into statements. 

Scale testing indicates that merging the changes into the single control table is the limiting factor for the project. This places a practical limit on project's support (not Snowflake's) for parallel execution of approximately 40 warehouses executing the stored procedure in parallel. The largest recommended size per warehouse for this project is medium, which provide 32 processing cores. 40 medium warehouses times 32 processing cores provides an upper limit for this project of 1280 cores. Since Snowflake ingests one file at a time per core, this project supports a practical upper limit of ingesting 1280 files in parallel. (An update to this project will eliminate the practical limit on 40 parallel executing stored procedures.)
