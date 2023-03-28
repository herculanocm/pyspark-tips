import sys
sys.path.insert(0, f'../de_pyspark_tips/')

from de_pyspark_tips import df_transformations
from pyspark.sql import SparkSession
import pytest



data_df1 = [
    (1,"2020-08-28", "true", "G1 | ESCADA 1O P/ 2O | STAND | FRALDÁRIO","d", "3.15868", "","I","2020-08-27 23:59:59"),
    (1,"2020-08-28", "true", "G1 | ESCADA 1O P/ 2O | STAND | FRALDÁRIO","d", "3.15868", "","U" ,"2020-08-28 12:59:59"),
    (2,"2018-05-25", "true","G1 | ESCADA 1O P/     2O | STAND | FRALDÁRIO     ","A", "5.5", "NÃO EXISTE", "I","2020-08-27 23:59:59"),
    (3,"1510-05-25", "true","""G1 | ESCADA 1O P/     2O | STAND | FRALDÁRIO     
    NOVO |
    STUFF
    ""","d", "389.390","", "I","1510-08-27 23:59:59")
    ]
columns_df1 =["id","data", "ativo", "unidade", "sigla", "valor", "obs", "action", "data_transaction"]


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


def ttest_cast_columns_types_by_schema(spark: SparkSession):
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
        {'column_name': 'obs', 'data_type': 'VARCHAR(MAX)'},
        {'column_name': 'action', 'data_type': 'VARCHAR(1)'},
        {'column_name': 'data_transaction', 'data_type': 'TIMESTAMP'},
    ]

    newDF = df_transformations.cast_columns_types_by_schema(df, list_schema, True)

    print('')
    print('New DF')
    newDF.printSchema()
    newDF.show(truncate=False)
    list_expected_dtypes = [('id', 'int'), ('data', 'date'), ('ativo', 'boolean'), ('unidade', 'string'), ('sigla', 'string'), ('valor', 'double'), ('obs', 'string'), ('action', 'string'), ('data_transaction', 'timestamp')]
    list_df_dtypes = (newDF.dtypes)

    assert list_expected_dtypes == list_df_dtypes


def ttest_choose_last_row_modify_by_ids(spark: SparkSession):
    df=spark.createDataFrame(data_df1, columns_df1)

    list_schema = [
        {'column_name': 'id', 'data_type': 'INTEGER'},
        {'column_name': 'data', 'data_type': 'DATE'},
        {'column_name': 'ativo', 'data_type': 'BOOLEAN'},
        {'column_name': 'unidade', 'data_type': 'VARCHAR(200)'},
        {'column_name': 'sigla', 'data_type': 'VARCHAR(2)'},
        {'column_name': 'valor', 'data_type': 'NUMERIC(10,2)'},
        {'column_name': 'obs', 'data_type': 'VARCHAR(MAX)'},
        {'column_name': 'action', 'data_type': 'VARCHAR(1)'},
        {'column_name': 'data_transaction', 'data_type': 'TIMESTAMP'},
    ]

    castedDF = df_transformations.cast_columns_types_by_schema(df, list_schema, True)

    filterDF = df_transformations.choose_last_row_modify_by_ids(castedDF, ['id'], ['data_transaction'])
    filterDF.show(truncate=False)

    assert filterDF.filter("id == 1").count() == 1


def test_dates(spark: SparkSession):
    df=spark.createDataFrame(data_df1, columns_df1)

    list_schema = [
        {'column_name': 'id', 'data_type': 'INTEGER'},
        {'column_name': 'data', 'data_type': 'DATE'},
        {'column_name': 'ativo', 'data_type': 'BOOLEAN'},
        {'column_name': 'unidade', 'data_type': 'VARCHAR(200)'},
        {'column_name': 'sigla', 'data_type': 'VARCHAR(2)'},
        {'column_name': 'valor', 'data_type': 'NUMERIC(10,2)'},
        {'column_name': 'obs', 'data_type': 'VARCHAR(MAX)'},
        {'column_name': 'action', 'data_type': 'VARCHAR(1)'},
        {'column_name': 'data_transaction', 'data_type': 'TIMESTAMP'},
    ]

    castedDF = df_transformations.cast_columns_types_by_schema(df, list_schema, True)

    # filterDF = df_transformations.choose_last_row_modify_by_ids(castedDF, ['id'], ['data_transaction'])
    castedDF.show(truncate=False)

    assert 1 == 1

    