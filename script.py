from pyspark.sql import SparkSession
import sys

usr = sys.argv[1]
pw = sys.argv[2]
db = "jdbc:mysql://" + sys.argv[3] + sys.argv[4]

if __name__ == "__main__":

    spark = SparkSession \
             .builder \
             .appName("PythonLoadAW2014") \
             .getOrCreate()

    table_list = spark \
              .read \
              .format("jdbc") \
              .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/") \
              .option("driver", "com.mysql.jdbc.Driver") \
              .option("dbtable", "information_schema.tables") \
              .option("user", "guest") \
              .option("password", "relational") \
              .load() \
              .filter("table_schema = 'AdventureWorks2014'") \
              .select("table_name")

    table_array = [row.table_name for row in table_list.toLocalIterator()]

    for i in table_array:

        if i in ["AWBuildVersion", "CurrencyRate", "EmployeePayHistory", "ProductReview", "ProductVendor", "PurchaseOrderDetail", "SalesPersonQuotaHistory", "ShoppingCartItem", "TransactionHistory", "TransactionHistoryArchive"]:
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i in ["BillOfMaterials", "ProductCostHistory", "ProductListPriceHistory", "SalesTerritoryHistory", "SpecialOffer"]:
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "EndDate CHAR(21), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "Address":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", "(SELECT AddressID, AddressLine1, AddressLine2, City, StateProvinceID, PostalCode, ST_AsWKT(SpatialLocation) AS SpatialLocation, rowguid, ModifiedDate FROM Address) as Address") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "Product":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "SellEndDate CHAR(21), DiscontinuedDate CHAR(21), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "PurchaseOrderHeader":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "ShipDate CHAR(21), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "SalesOrderHeader":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "DueDate CHAR(21), ShipDate CHAR(21), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "Shift":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "StartTime CHAR(8), EndTime CHAR(8), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "WorkOrder":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "EndDate CHAR(21), DueDate CHAR(21), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "WorkOrderRouting":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "ScheduledEndDate CHAR(21), ActualStartDate CHAR(21), ActualEndDate CHAR(21), ModifiedDate CHAR(21)") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        else:
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        load_table \
                .write \
                .format("jdbc") \
                .option("url", db) \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("user", usr) \
                .option("password", pw) \
                .save(mode="ignore")
