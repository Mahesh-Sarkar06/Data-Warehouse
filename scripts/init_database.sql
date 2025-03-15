/*
======================================================================================================================================================================================
                                                                          CREATING DATABASE & SCHEMA
======================================================================================================================================================================================

NOTE: Run the commands one by one. On terminal, always add semi-colon (;) to run the command. Until semi-colon isn't added, it will consider the command is continuing.
*/



-- Open terminal/GUI for pgSQL

-- CREATING A NEW DATABASE
CREATE DATABASE datawarehouse;

-- Connect to the database 'datawarehouse'
-- From terminal
\c datawarehouse;

-- From GUI
-- 1) Select the database 'datawarehouse' on the left panel under database drop down, if not visible then refresh the list.
-- 2) Then click on db icon on top left corner of the panel. This will connect to the database and open a new script page.

-- CREATING SCHEMAS
CREATE SCHEMA bronze;    -- Bronze layer tables schema
CREATE SCHEMA silver;    -- Silver layer tables schema
CREATE SCHEMA gold;      -- Gold layer VIEWS schema
