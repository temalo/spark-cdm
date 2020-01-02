# Getting Started with spark-cdm
This library provides the capability to write [CDM Folders](https://docs.microsoft.com/en-us/common-data-model/data-lake) from within Azure Databricks. If you are unfamilar with the Common Data Model and the use case for integration with various Azure Data Platform offerings, see the excellent series of articles by [Matthew Roche](https://twitter.com/SQLAllFather) on his blog [here](https://ssbipolar.com/category/power-bi/dataflows/).

This code is based on [spark-cdm](https://github.com/Azure/spark-cdm) but has been modified and extended to support large datasets. My intention is to further extend this library by adding the capability to write lineage information directly into [Apache Atlas](http://atlas.apache.org/0.8.0-incubating/index.html) which is the foundation for Azure Data Catalog Gen2.

## Configuring Access to Azure Data Lake Store (Gen2)
In order to access resources in Azure, such as Azure Data Lake Store (ADLS), you should configure a [Service Principal](https://docs.microsoft.com/en-us/azure/active-directory/develop/app-objects-and-service-principals) that will be used to grant access. Once you do this (If you are unfamiliar with creating a Service Principal, you can follow the steps [here](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) to create one.) Keep in mind that you MUST copy the client secret as you will only be able to view it during the creation process. You will need the following 3 things from the object that you just created:
1. The Application ID (also known as a "Client ID"), which will be in the form of a GUID
2. The Tenant ID, which will be in the form of a GUID (Note: You do not need the full URL as you would in some ADLS itegrations, just the GUID)
3. The Client Secret (sometimes called a Token). Note that this will ONLY be available during the creation process

Once the service principal has been created, you will need to assign permissions to it in your ADLS account. The recommended way is to use [Azure RBAC](https://docs.microsoft.com/en-us/azure/role-based-access-control/overview) controls and assign the role [Storage Blob Data Contributor](https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-rbac-portal) to your ADLS Account. (See the previous link if you are unfamiliar with how to do this)
## Configuring your Azure Databricks cluster to support spark-cdm
The only requirement for this library is that it be installed directly on the cluster. The easiest way to do this is by following [these](https://docs.microsoft.com/en-us/azure/databricks/libraries#install-workspace-libraries) instructions to install a "Workspace Library". 

## Using spark-cdm to create a CDM Folder
The following example is based on a dataset that I detailed in my [blog](https://sqltrainer.com/2019/04/07/update-to-creating-a-real-world-database-in-azure/) and is assuming that an ADLS Gen2 account named "temalo" that contains a filesystem named "adlsgen2" exists and that the folder "CDMFolders/Databricks" is the location where you wish to write the CDM Folders for the data model. 

Although the spark-cdm library is written in Scala, it can be used in either python or Scala code within a Databricks notebook. The basic process is as follows:
1. Configure the CDM Environment
    1. Set the Output path
    2. Set the Model Name
    3. Obtain the AppID, Secret, and TenantID from the Service Principal
2. Populate a Spark Table
3. Write the Spark Table (Entity) to the CDM Folder

### Configuring the CDM Environment
The following code assumes that you have configured a [Databricks Secrets Scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secrets) that includes all of the required information to identify and use the Service Principal discussed above.

    %python
    cdmModelName = "Chicago Crime"
    outputLocation = "https://temalo.dfs.core.windows.net/adlsgen2/CDMFolders/Databricks" 
    appID = dbutils.secrets.get(scope="AKV", key="AppID")
    appKey = dbutils.secrets.get(scope="AKV", key="Token")
    tenantID = dbutils.secrets.get(scope = "AKV", key="TenantID")

### Populating a Spark Table (or tables)

The following code assumes that you have data stored in an Azure SQL Database instance that mirrors the data from the blog post referenced above (note that the query used will return 10 years worth of Crime Reports if you have followed the steps in the blog to create a complete dataset)

    %python
    jdbcUsername = dbutils.secrets.get(scope = "AKV", key = "dbUsername")
    jdbcPassword = dbutils.secrets.get(scope = "AKV", key = "dbPassword")
    #Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    jdbcHostname = "tmdemo.database.windows.net"
    jdbcPort = 1433
    jdbcDatabase ="ChicagoCrime"

    #Create the JDBC URL without passing in the user and password parameters.
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
      connectionProperties = {
      "user" : jdbcUsername,
      "password" : jdbcPassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    db_query = "(SELECT [Case Number] ,[Date] ,[Block] ,[IUCR] ,[Primary Type] ,[Description] ,[Location Description] ,[Arrest] ,[Domestic] ,[Beat] ,[District] ,[Ward] ,[Community Area] ,[FBI Code] ,[Latitude] ,[Longitude] ,             [DateOnly] FROM ChicagoCrimes WHERE DATEDIFF(yy,DateOnly,GetUTCDate()) <= 10) result"
    result = spark.read.jdbc(url=jdbcUrl, table=db_query, properties=connectionProperties)
    result.createOrReplaceTempView("ChicagoCrimeReports")

### Write the Spark Table to a CDM Folder

Once the data is stored in a Spark Table, you can call the write.format method on the table, and pass "com.microsoft.cdm" as the format, supplying the required options as shown:

    %python
    ChicagoCrimeReports = spark.table("ChicagoCrimeReports")
    (ChicagoCrimeReports.write.format("com.microsoft.cdm")
                       .option("entity", "Crime Reports")
                       .option("appId", appID)
                       .option("appKey", appKey)
                       .option("tenantId", tenantID)
                       .option("cdmFolder", outputLocation)
                       .option("cdmModelName", cdmModelName)
                       .save())

## Using spark-cdm to read a CDM Folder
Reading from a CDM folder is relatively straightforward. The process is exactly as described above, except you call the read.format method