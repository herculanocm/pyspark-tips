import sys
sys.path.insert(0, f'../de_pyspark_tips/')

from de_pyspark_tips import df_transformations
from pyspark.sql import SparkSession
import pytest



data_df1 = [
    (1,"2020-08-28", "true", "G1 | ESCADA 1O P/ 2O | STAND | FRALDÁRIO","d", "3.15868", ""),
    (2,"2018-05-25", "true","G1 | ESCADA 1O P/     2O | STAND | FRALDÁRIO     ","A", "5.5", "NÃO EXISTE"),
    (3,"2018-05-25", "true","""G1 | ESCADA 1O P/     2O | STAND | FRALDÁRIO     
    NOVO |
    STUFF
    ""","d", "389.390","")
    ]
columns_df1 =["id","data", "ativo", "unidade", "sigla", "valor", "obs"]


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_cast_columns_types_by_schema(spark):
    print('')
    print('Original DF')
    df=spark.createDataFrame(data_df1, columns_df1)
    df.printSchema()

    list_schema = [
        {'column_name': 'id', 'data_type': 'INTEGER'},
        {'column_name': 'data', 'data_type': 'DATE'},
        {'column_name': 'ativo', 'data_type': 'BOOLEAN'},
        {'column_name': 'unidade', 'data_type': 'VARCHAR(200)'},
        {'column_name': 'sigla', 'data_type': 'VARCHAR(2)'},
        {'column_name': 'valor', 'data_type': 'NUMERIC(10,2)'},
        {'column_name': 'obs', 'data_type': 'VARCHAR(MAX)'}
    ]

    newDF = df_transformations.cast_columns_types_by_schema(df, list_schema, True)

    print('')
    print('New DF')
    newDF.printSchema()
    newDF.show(truncate=False)
    list_expected_dtypes = [('id', 'int'), ('data', 'date'), ('ativo', 'boolean'), ('unidade', 'string'), ('sigla', 'string'), ('valor', 'double'), ('obs', 'string')]
    list_df_dtypes = (newDF.dtypes)

    assert list_expected_dtypes == list_df_dtypes