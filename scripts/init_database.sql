/*
Create Database and Schema

Script purpose:
  This script creates a new database called DataWarehouse before that checking if the database is already present in the SQL Server.
  For the project sake if the database is presented I just drop the database and recreated.
  In addiiton we creating three schema called bronze, silver and gold.

*/



USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF exists (Select 1 from sys.database where name = 'DataWarehouse')
Begin 
    Alter database DataWarehouse set single_user with roleback immediate;
    Drop database DataWarehouse;

end;
Go

-- create the 'DataWarehouse' database
create Database DataWarehouse;
Go

USE DataWarehouse;
Go

-- create Schema
Create schema bronze;
Go

Create schema silver;
Go

Create schema gold;
Go

