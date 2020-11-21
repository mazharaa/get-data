from pyspark.sql import SparkSession
import sys

usr = sys.argv[1]
pw = sys.argv[2]
# db = sys.argv[3]
# local_url = "jdbc:mysql://localhost:3306/"+db

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("PythonLoadAW2014")
             .getOrCreate())

    table_list = (spark
              .read
              .format("jdbc")
              .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/")
              .option("driver", "com.mysql.jdbc.Driver")
              .option("dbtable", "information_schema.tables")
              .option("user", "guest")
              .option("password", "relational")
              .load()
              .filter("table_schema = 'AdventureWorks2014'")
              .select("table_name"))

    table_array = [row.table_name for row in table_list.toLocalIterator()]

    for i in table_array:
        if i == "Address":
            load_table = (spark
                .read
                .format("jdbc")
                .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", i)
                .option("user", "guest")
                .option("password", "relational")
                .load())

            (load_table
                .write
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/test_db")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", i)
                .option("user", usr)
                .option("password", pw)
                .save())

    # loadDB = (spark
    #         .read
    #         .format("jdbc")
    #         .option("url", "jdbc:mysql://relational.fit.cvut.cz:3306/AdventureWorks2014")
    #         .option("driver", "com.mysql.jdbc.Driver")
    #         .option("user", "guest")
    #         .option("password", "relational"))
    # for tablename in table_list:
    #     loadDB.option("dbtable", tablename).load().createTempView(tablename)
