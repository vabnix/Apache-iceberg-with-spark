from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Initialize Spark with Iceberg
spark = SparkSession.builder \
    .appName("IcebergDemo") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0") \
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/warehouse") \
    .config("spark.sql.defaultCatalog", "local") \
    .getOrCreate()

print("Spark session created successfully!")

# Create sample data with more fields
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("hire_date", TimestampType(), True)
])

data = [
    (1, "Vaibhav Srivastava", "Engineering", 85000, datetime.now()),
    (2, "Aum Srivastava", "Marketing", 75000, datetime.now()),
    (3, "Ishaan Srivastava", "Sales", 80000, datetime.now())
]

print("Creating DataFrame...")
df = spark.createDataFrame(data, schema)

print("Writing initial data to Iceberg table...")
# Create namespace and table
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
df.createOrReplaceTempView("temp_employees")
spark.sql("""
    CREATE OR REPLACE TABLE local.db.employees 
    USING iceberg 
    AS SELECT * FROM temp_employees
""")

# Demonstrate schema evolution
print("\nAdding a new column 'bonus'...")
spark.sql("""
    ALTER TABLE local.db.employees 
    ADD COLUMN bonus INT AFTER salary
""")


# Add new data with the updated schema
new_data = [
    (4, "Pallabi Chakraborty", "HR", 70000, 10000, datetime.now()),
    (5, "Kartik Srivastava", "Engineering", 90000, 15000, datetime.now())
]

new_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("bonus", IntegerType(), True),
    StructField("hire_date", TimestampType(), True)
])

new_df = spark.createDataFrame(new_data, new_schema)
new_df.createOrReplaceTempView("new_employees")

print("Adding new records with bonus data...")
spark.sql("""
    INSERT INTO local.db.employees 
    SELECT * FROM new_employees
""")

# Demonstrate time travel capabilities
print("\nCurrent table contents:")
spark.sql("SELECT * FROM local.db.employees").show()

print("\nTable history:")
spark.sql("SELECT * FROM local.db.employees.history").show()

# Demonstrate table snapshots
print("\nTable snapshots:")
spark.sql("SELECT * FROM local.db.employees.snapshots").show()

# Demonstrate metadata queries with correct column names
print("\nTable metadata:")
spark.sql("""
    SELECT
        s.snapshot_id,
        s.parent_id,
        s.operation,
        s.committed_at,
        s.manifest_list
    FROM local.db.employees.snapshots s
""").show(truncate=False)

# Demonstrate time travel query using snapshot ID
print("\nDemonstrating time travel...")
spark.sql("""
    SELECT snapshot_id 
    FROM local.db.employees.history 
    ORDER BY made_current_at 
    LIMIT 1
""").show()

# Add some data manipulation examples
print("\nDepartment-wise salary statistics:")
spark.sql("""
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary,
        AVG(bonus) as avg_bonus
    FROM local.db.employees
    GROUP BY department
""").show()

print("\nDemo completed successfully!")
spark.stop()