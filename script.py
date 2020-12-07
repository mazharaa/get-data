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

        if i == "AWBuildVersion":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "VersionDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "CurrencyRate":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "CurrencyRateDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "EmployeePayHistory":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "RateChangeDate date") \
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
                .option("customSchema", "SellStartDate date, SellEndDate date, DiscontinuedDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "ProductReview":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "ReviewDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "ProductVendor":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "LastReceiptDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()
        
        elif i == "PurchaseOrderDetail":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "DueDate date") \
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
                .option("customSchema", "OrderDate date, ShipDate date") \
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
                .option("customSchema", "OrderDate date, DueDate date, ShipDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "SalesPersonQuotaHistory":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "QuotaDate date") \
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
                .option("customSchema", "StartTime string, EndTime string") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i == "ShoppingCartItem":
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "DateCreated date") \
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
                .option("customSchema", "StartDate date, EndDate date, DueDate date") \
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
                .option("customSchema", "ScheduledStartDate date, ScheduledEndDate date, ActualStartDate date, ActualEndDate date") \
                .option("user", "guest") \
                .option("password", "relational") \
                .load()

        elif i in ["TransactionHistory", "TransactionHistoryArchive"]:
            load_table = spark \
                .read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", i) \
                .option("customSchema", "TransactionDate date") \
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
                .option("customSchema", "StartDate date, EndDate date") \
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
