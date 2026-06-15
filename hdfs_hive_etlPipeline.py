from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import logging
import sys


class HDFSToHivePipeline:

    # -----------------------
    # INIT
    # -----------------------
    def __init__(self, config_path):
        self.logger = self.get_logger()
        self.config = self.read_config(config_path)
        self.spark = self.create_spark_session(self.config.get("app_name"))

    # -----------------------
    # LOGGER
    # -----------------------
    def get_logger(self):
        logger = logging.getLogger("HDFS_ETL")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(handler)

        return logger

    # -----------------------
    # READ CONFIG
    # -----------------------
    def read_config(self, path):
        try:
            with open(path, "r") as f:
                config = json.load(f)
                self.logger.info("Config loaded successfully")
                return config
        except Exception as e:
            self.logger.error("Error reading config: {}".format(str(e)))
            sys.exit(1)

    # -----------------------
    # CREATE SPARK (Hive enabled)
    # -----------------------
    def create_spark_session(self, app_name):
        try:
            spark = SparkSession.builder \
                .appName(app_name) \
                .enableHiveSupport() \
                .getOrCreate()

            self.logger.info("Spark session created with Hive support")
            return spark

        except Exception as e:
            self.logger.error("Error creating Spark session: {}".format(str(e)))
            sys.exit(1)

    # =====================================================
    # 🔹 1. EXTRACT (Read from HDFS)
    # =====================================================
    def extract(self):
        try:
            self.logger.info("🔹 Extract phase started")

            input_path = self.config.get("input_path")

            if not input_path:
                raise ValueError("Input path missing in config")

            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)

            self.logger.info("Data read successfully from HDFS")

            print("\n===== HDFS DATA SAMPLE =====")
            df.show(10, truncate=False)

            return df

        except Exception as e:
            self.logger.error("Extract failed: {}".format(str(e)))
            raise

    # =====================================================
    # 🔹 2. TRANSFORM
    # =====================================================
    def transform(self, df):
        try:
            self.logger.info("🔹 Transform phase started")

            df = df.withColumn(
                "txn_category",
                when(col("amount") > 10000, "High")
                .when(col("amount") > 5000, "Medium")
                .otherwise("Low")
            )

            df = df.withColumn("date", date_format(to_date(col("date"), "yyyy-MM-dd"), "yyyy-MM-dd"))

            df = df.withColumn("processed_time", current_timestamp())

            df = df.withColumnRenamed("txn_id", "transaction_id")

            self.logger.info("Transformation completed")

            print("\n===== TRANSFORMED DATA =====")
            df.show(10, truncate=False)

            return df

        except Exception as e:
            self.logger.error("Transform failed: {}".format(str(e)))
            raise

    # =====================================================
    # 🔹 3. LOAD (Write to Hive)
    # =====================================================
    def load(self, df):
        try:
            self.logger.info("🔹 Load phase started")

            db = self.config.get("hive_database")
            table = self.config.get("hive_table")

            if not db or not table:
                raise ValueError("Hive database or table missing in config")

            full_table = "{}.{}".format(db, table)

            # Create DB if not exists
            self.spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db))

            # Write to Hive
            df.write \
                .mode("overwrite") \
                .saveAsTable(full_table)

            self.logger.info("Data loaded into Hive table: {}".format(full_table))

            print("\n🎯 Data successfully loaded into Hive table: {}".format(full_table))

        except Exception as e:
            self.logger.error("Load failed: {}".format(str(e)))
            raise

    # =====================================================
    # RUN PIPELINE
    # =====================================================
    def run(self):
        try:
            self.logger.info("🚀 Pipeline started")

            df = self.extract()
            df = self.transform(df)
            self.load(df)

            self.logger.info("🎉 Pipeline completed successfully")

        except Exception as e:
            self.logger.error("Pipeline failed: {}".format(str(e)))
            sys.exit(1)

        finally:
            self.spark.stop()
            self.logger.info("Spark session stopped")


# -----------------------
# ENTRY POINT (IMPORTANT)
# -----------------------
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: spark-submit script.py <config_file>")
        sys.exit(1)

    config_path = sys.argv[1]

    pipeline = HDFSToHivePipeline(config_path)
    pipeline.run()