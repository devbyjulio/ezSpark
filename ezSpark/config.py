import os

# Base Directory (the directory of this config.py file)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Paths to JAR files
JARS_DIR = os.path.join(BASE_DIR, 'jars')

# List of JAR files required
DEPENDENCY_JARS = [
    os.path.join(JARS_DIR, 'mssql-jdbc-12.8.1.jre8.jar'),
    os.path.join(JARS_DIR, 'spark-excel_2.12-0.14.0.jar'),
    os.path.join(JARS_DIR, 'poi-5.2.3.jar'),
    os.path.join(JARS_DIR, 'poi-ooxml-schemas-4.1.2.jar'),
    os.path.join(JARS_DIR, 'commons-codec-1.15.jar'),
    os.path.join(JARS_DIR, 'commons-collections4-4.4.jar'),
    os.path.join(JARS_DIR, 'commons-math3-3.6.1.jar'),
    os.path.join(JARS_DIR, 'xmlbeans-3.1.0.jar'),
    os.path.join(JARS_DIR, 'curvesapi-1.06.jar'),
    os.path.join(JARS_DIR, 'scala-xml_2.12-1.3.0.jar'),
    # Add other dependency JARs as needed
]

# Path to temporary directory
BASE_TEMP = os.path.join(BASE_DIR, 'temp')

# Combine all JAR paths into a single string
ALL_JARS_STR = ','.join(DEPENDENCY_JARS)
