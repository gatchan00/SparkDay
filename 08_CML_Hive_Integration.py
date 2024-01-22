import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
#from pyspark import SparkContext
#SparkContext.setSystemProperty('spark.executor.cores', '1')
#SparkContext.setSystemProperty('spark.executor.memory', '2g')

CONNECTION_NAME = "sonaemc-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "show databases"
spark.sql(EXAMPLE_SQL_QUERY).show()

df2=spark.sql("select * from default.curated")
df2.show()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "SELECT contract, count(*) as cant  FROM default.curated GROUP BY contract"
temp_df = spark.sql(EXAMPLE_SQL_QUERY)
temp_df.write.mode("overwrite").saveAsTable("default.curated_0101")

df2=spark.sql("show tables")
df2.show()