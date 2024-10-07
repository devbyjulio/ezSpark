# ezSpark

ezSpark is a Python package designed to simplify the process of uploading data from Excel or CSV files to a Microsoft SQL Server (MSSQL) database using Apache Spark. It provides a flexible and efficient way to handle large datasets, with support for custom Spark configurations and data transformations.

## Features

- **Multi-Format Support**: Upload data from Excel (.xlsx) and CSV files.
- **Scalable Processing**: Leverage Apache Spark for efficient handling of large datasets.
- **Customizable Mappings**: Map columns from source files to target database columns.
- **Data Type Casting**: Specify data types for each column to ensure data integrity.
- **Configurable Spark Settings**: Customize Spark configurations like memory allocation and partitioning.

## Installation

### Prerequisites

- **Python**: Version 3.6 or higher
- **Apache Spark**: Compatible with your Python and Java versions
- **Microsoft JDBC Driver for SQL Server**: https://go.microsoft.com/fwlink/?linkid=2283744
- **Java jdk-11 (must create oracle account for download)**: https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html#license-lightbox
- **Hadoop - winutils.exe**: https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/winutils.exe

### Clone the Repository

```bash
git clone https://github.com/yourusername/ezSpark.git
cd EZSPARK
```
### Install Dependencies
```bash
pip install -r requirements.txt
```
### Set Up Environment Variable

- **JAVA_HOME**:
    1. win + r -> SystemPropertiesAdvanced
    2. Go to environment variables
    3. Add new system variable
    4. Variable name: JAVA_HOME
    5. Variable value: "C:your\path\to\jdk-11"
    6. Edit the 'Path' system variable
    7. Add a new 'Path' variable: %JAVA_HOME%\bin
    8. Ok -> Ok
    9. Restart computer

Test setup
```bash
echo %JAVA_HOME%
```

- **HADOOP_HOME**:
    1. Create a folder for hadoop files (e.g. "C:\hadoop\bin") if hadoop is not already installed
    2. Put 'winutils.exe' inside
    3. win + r -> SystemPropertiesAdvanced
    4. Go to environment variables
    5. Add new system variable
    6. Variable name: HADOOP_HOME
    7. Variable value: "C:your\path\to\hadoop\bin"
    8. Edit the 'Path' system variable
    9. Add a new 'Path' variable: %HADOOP_HOME%\bin
    10. Ok -> Ok
    11. Restart computer

Test setup
```bash
echo %HADOOP_HOME%
```

## Usage
An example script example.py is provided to demonstrate how to use the package.

## Steps to Use

1. **Set Up Your Data File**:  Place your Excel or CSV file in an accessible location and note its path.

2. **Configure Database Details**: Update the db_details dictionary with your database connection information.

3. **Map Columns**: Define a column_mapping dictionary to map your source file columns to the target database columns.

4. **Specify Data Types**: Create a column_types dictionary to cast columns to the appropriate data types.

5. **Customize Optional Parameters**:

    - **sheet_name**: For Excel files, specify the sheet name if it's not "Sheet1".
    - **spark_configs**: Adjust Spark configurations for performance tuning.
    - **repartition_num**: Set the number of partitions for the dataset.
    - **date_formats**: Set date formats when uploading csv files. If not set, will upload null.

6. **Run the Script**: Execute your script to start the data upload process.

## Limitations

- **Database Support**: Currently, only Microsoft SQL Server (mssql) is supported. Support for other databases can be added in future versions.
- **File Type Support**: Only Excel (.xlsx) and CSV files are supported at this time.
- **Operating System**: The project is configured for Windows systems. Modifications may be needed for Unix-based systems.
- **Error Handling**: Limited error handling is implemented. Users should verify configurations to prevent runtime errors.
- **Dependency Management**: The project includes specific versions of JAR files and may require adjustments for compatibility with other versions.

## Configuration Details
### config.py

The config.py file contains crucial configuration settings:

- **Base Directories**: Sets the base path for the project and libraries.
- **JAR Dependencies**: Lists the required JAR files needed by Spark for JDBC connections and Excel file processing.

Ensure that the paths and files specified in config.py are accurate and exist on your system.

## Extending the Project

### Adding Support for Other Databases

To add support for additional databases:

1. **Update `DatabaseFactory`**: Modify the `get_database_uploader` method in `uploader.py` to include new database types and their JDBC drivers.
2. **Include JDBC JARs**: Add the necessary JDBC driver JAR files to the `libs/jars` directory and update the `DEPENDENCY_JARS` list in `config.py`.
3. **Test Connections**: Verify that the uploader can successfully connect and write to the new database type.

### Enhancing File Type Support

To support more file types:

1. **Create a New Uploader Class**: Implement a new class that extends `DataUploader` for the new file type.
2. **Implement `load_data_to_dataframe`**: Define how the new file type is read into a Spark DataFrame.
3. **Update `DatabaseFactory`**: Add logic to return the new uploader class based on the file type.

## Troubleshooting

- **Missing JAR Files**: Ensure all JAR files listed in `config.py` exist in the `libs/jars` directory.
- **Environment Variable Errors**: Verify that `JAVA_HOME` and `HADOOP_HOME` are set correctly.
- **Windows Auth for SQL**: if you are using windows authenticator, you'll need to copy the 'mssql-jdbc_auth-12.8.1.x64.dll' from the JDBC Driver ('auth\x64') to the 'jdk-11\bin' directory.
- **Spark Session Errors**: Check Spark configurations and ensure compatibility with your Spark installation.
- **Database Connection Issues**: Confirm database credentials and network connectivity.


## License
This project is licensed under the MIT License. You are free to use, modify, and distribute this software.

## Acknowledgments
- **Apache Spark**: For providing a powerful data processing engine.