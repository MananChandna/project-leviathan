from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main():
    spark = SparkSession.builder \
        .appName("AssetInventoryToNeo4j") \
        .getOrCreate()

    print("Spark Session created. Reading asset inventory data...")

    # 1. Read the iam_roles.json file
    try:
        asset_df = spark.read.option("multiLine", True).json("../../cloud-asset-inventory/iam_roles.json")
        print("Successfully read iam_roles.json")
    except Exception as e:
        print(f"Error reading asset data. Make sure 'iam_roles.json' exists. Error: {e}")
        spark.stop()
        return

    # 2. Transform the data for the graph model
    nodes_df = asset_df.withColumn("labels", lit("IAMRole")) \
                       .withColumnRenamed("RoleName", "name") \
                       .withColumnRenamed("Arn", "arn")

    print("Transformed data into nodes format:")
    nodes_df.show(5)

    # 3. Write the DataFrame to Neo4j
    print("Writing nodes to Neo4j...")
    try:
        nodes_df.write \
            .format("org.neo4j.spark.DataSource") \
            .mode("Overwrite") \
            .option("url", "neo4j://192.168.49.2:32725") \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", "neo4j") \
            .option("authentication.basic.password", "please-change-this-password") \
            .option("labels", ":IAMRole") \
            .option("node.properties", "name,arn") \
	    .option("node.keys", "arn") \
            .save()
        print("Successfully wrote data to Neo4j.")
    except Exception as e:
        print(f"An error occurred while writing to Neo4j: {e}")

    spark.stop()

if __name__ == '__main__':
    main()
