from pyspark.sql import SparkSession
from pyspark.sql.types import *
from abc import ABC, abstractmethod
import os, shutil
from . import config

class DataUploader(ABC):
    def __init__(self, file_path, db_url, db_table, db_user, db_password, db_driver,
                 spark_configs=None, repartition_num=None):
        """
        Initialize the uploader with paths and connection details.
        :param file_path: Path to the source file
        :param db_url: The JDBC URL for the SQL database
        :param db_table: The target table in the SQL database
        :param db_user: Username for the SQL database
        :param db_password: Password for the SQL database
        :param db_driver: JDBC driver for the SQL database
        :param spark_configs: Optional dictionary of Spark configurations
        :param repartition_num: Optional integer for the number of partitions
        """
        self.file_path = file_path
        self.db_url = db_url
        self.db_table = db_table
        self.db_user = db_user
        self.db_password = db_password
        self.db_driver = db_driver
        self.spark_configs = spark_configs or {}
        self.repartition_num = repartition_num

        self.spark = self._initialize_spark_session()

    def _initialize_spark_session(self):
        """
        Initialize and return a Spark session with the necessary JARs.
        """

        # Check that all JAR files exist
        missing_jars = [jar for jar in config.DEPENDENCY_JARS if not os.path.isfile(jar)]
        if missing_jars:
            raise FileNotFoundError(f"The following JAR files are missing: {', '.join(missing_jars)}")

        # Default Spark configurations
        default_configs = {
            "spark.jars": config.ALL_JARS_STR,
            "spark.driver.memory": "8g",
            "spark.executor.memory": "8g",
            "spark.sql.debug.maxToStringFields": "1000",
            "spark.sql.files.maxPartitionBytes": "64m",
            "spark.local.dir": config.BASE_TEMP,
            "spark.cleaner.referenceTracking.cleanCheckpoints": "true",
            "spark.cleaner.referenceTracking.blocking": "true",
            "spark.cleaner.periodicGC.interval": "60s",
        }

        # Update default configs with any user-provided configs
        default_configs.update(self.spark_configs)

        # Build SparkSession
        builder = SparkSession.builder.appName("Data Uploader").master("local[*]").config("spark.hadoop.fs.defaultFS", "file:///")
        for key, value in default_configs.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    @abstractmethod
    def load_data_to_dataframe(self):
        """
        Abstract method to load data into a Spark DataFrame.
        """
        pass

    def map_columns(self, spark_df, column_mapping):
        """
        Map source file columns to SQL columns based on the provided mapping.
        """
        select_expr = [f"`{source_col}` as `{sql_col}`" for source_col, sql_col in column_mapping.items()]
        return spark_df.selectExpr(*select_expr)

    def cast_columns(self, mapped_df, column_types):
        """
        Cast the columns to specified data types.
        """
        for column, dtype in column_types.items():
            mapped_df = mapped_df.withColumn(column, mapped_df[column].cast(dtype))
        return mapped_df

    def upload_to_sql(self, spark_df):
        """
        Upload the mapped DataFrame to the SQL database.
        """
        # Start building the DataFrame writer
        writer = spark_df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", self.db_table) \
            .option("driver", self.db_driver) \
            .mode("append")

        # Add the user option only if a user is provided
        if self.db_user:
            writer = writer.option("user", self.db_user)

        # Add the password option only if a password is provided
        if self.db_password:
            writer = writer.option("password", self.db_password)

        # Save the DataFrame to the database
        writer.save()

    def process_and_upload(self, column_mapping, column_types=None):
        """
        Process the file and upload the data to the SQL database.
        """
        df = self.load_data_to_dataframe()
        if self.repartition_num:
            df = df.repartition(self.repartition_num)
        mapped_df = self.map_columns(df, column_mapping)
        if column_types:
            mapped_df = self.cast_columns(mapped_df, column_types)

        self.upload_to_sql(mapped_df)

    def close(self):
        """
        Close the Spark session and restore environment variables.
        """
        self.spark.stop()

        # Clean up temp directory after upload
        if os.path.exists(config.BASE_TEMP):
            for filename in os.listdir(config.BASE_TEMP):
                file_path = os.path.join(config.BASE_TEMP, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.remove(file_path)  # Delete file or symlink
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Recursively delete directory and its contents
                except PermissionError:
                    print(f"Failed to delete {file_path}. You can delete the files manually, or they will be removed in subsequent executions.")
                
class ExcelUploader(DataUploader):
    def __init__(self, file_path, db_url, db_table, db_user, db_password, db_driver,
                 sheet_name=None, spark_configs=None, repartition_num=None):
        """
        Initialize the ExcelUploader with an optional sheet name.
        """
        self.sheet_name = sheet_name or "Sheet1"
        super().__init__(file_path, db_url, db_table, db_user, db_password, db_driver,
                         spark_configs=spark_configs, repartition_num=repartition_num)

    def load_data_to_dataframe(self):
        """
        Load Excel file data into a Spark DataFrame.
        """
        return self.spark.read.format("com.crealytics.spark.excel") \
            .option("sheetName", self.sheet_name) \
            .option("useHeader", "true") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .load(self.file_path)

class CsvUploader(DataUploader):
    def __init__(self, file_path, db_url, db_table, db_user, db_password, db_driver,
                 date_formats=None, spark_configs=None, repartition_num=None):
        """
        Initialize the CsvUploader.
        """
        self.date_formats = date_formats or {}
        super().__init__(file_path, db_url, db_table, db_user, db_password, db_driver,
                         spark_configs=spark_configs, repartition_num=repartition_num)

    def load_data_to_dataframe(self):
        """
        Load CSV file data into a Spark DataFrame.
        """
        return self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load(self.file_path)

    def cast_columns(self, mapped_df, column_types):
        """
        Cast the columns to specified data types, handling date formats if specified.
        """
        from pyspark.sql.functions import to_date, to_timestamp, col

        for column, dtype in column_types.items():
            if isinstance(dtype, DateType) or isinstance(dtype, TimestampType):
                date_format = self.date_formats.get(column)
                if date_format:
                    if isinstance(dtype, DateType):
                        mapped_df = mapped_df.withColumn(column, to_date(col(column), date_format))
                    elif isinstance(dtype, TimestampType):
                        mapped_df = mapped_df.withColumn(column, to_timestamp(col(column), date_format))
                else:
                    # Use default date format
                    if isinstance(dtype, DateType):
                        mapped_df = mapped_df.withColumn(column, to_date(col(column)))
                    elif isinstance(dtype, TimestampType):
                        mapped_df = mapped_df.withColumn(column, to_timestamp(col(column)))
            else:
                mapped_df = mapped_df.withColumn(column, mapped_df[column].cast(dtype))
        return mapped_df

class DatabaseFactory:
    @staticmethod
    def get_database_uploader(file_type, file_path, db_type, db_details,
                              sheet_name=None, date_formats=None, spark_configs=None, repartition_num=None):
        """
        Factory method to return the correct uploader class based on file type and database type.
        """
        if db_type == "mssql":
            db_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        # Add more databases as needed

        db_url = db_details.get("db_url")
        db_table = db_details.get("db_table")
        db_user = db_details.get("db_user")
        db_password = db_details.get("db_password")

        if file_type == "excel":
            return ExcelUploader(file_path, db_url, db_table, db_user, db_password, db_driver,
                                 sheet_name=sheet_name, spark_configs=spark_configs, repartition_num=repartition_num)
        elif file_type == "csv":
            return CsvUploader(file_path, db_url, db_table, db_user, db_password, db_driver,
                               date_formats=date_formats, spark_configs=spark_configs, repartition_num=repartition_num)
        else:
            raise ValueError("Unsupported file type!")
