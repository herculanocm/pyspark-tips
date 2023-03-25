import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType, DateType, TimestampType, IntegerType, BooleanType
from pyspark.sql import Window
from typing import List, Dict
import logging
from sys import stdout
from de_pyspark_tips.custom_formatter_logger import CustomFormatter

def get_logger(logger: logging.Logger = None) -> logging.Logger:
    if logger is None:

        handler = logging.StreamHandler(stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(CustomFormatter())

        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    return logger

def cast_columns_types_by_schema(df: DataFrame, list_schema: list, empty_columns_to_none: bool = False, logger=None) -> DataFrame:
    logger = get_logger(logger)
    if (df is None or list_schema is None) or (len(list_schema) == 0):
        raise ValueError('The values could by not null or empty')
    else: 
        for column in list_schema:
            fieldname = column['column_name']
            fieldtype = column['data_type'].lower()

            logger.info(f"Casting column : {column['column_name']} to data_type: {column['data_type']}")

            if 'int' in fieldtype and 'big' not in fieldtype:
                df = df.withColumn(fieldname, F.col(fieldname).cast(IntegerType()))

            elif 'bool' in fieldtype:
                df = df.withColumn(fieldname, F.col(fieldname).cast(BooleanType()))
            
            elif 'numeric' in fieldtype or 'decimal' in fieldtype or \
                'double' in fieldtype or 'float' in fieldtype or \
                'real' in fieldtype or 'money' in fieldtype or \
                'currency' in fieldtype:
                df = df.withColumn(fieldname, F.regexp_replace(F.col(fieldname), ",", '.'))\
                    .withColumn(fieldname, F.col(fieldname).cast(DoubleType()))

            elif 'date' == fieldtype:
                df = df.withColumn(fieldname,
                F.when(F.col(fieldname).like('%/%/%'), F.to_date(F.col(fieldname), 'dd/MM/yyyy'))
                .otherwise(F.date_format(F.col(fieldname), "yyyy-MM-dd")).cast(DateType())
                )
                    
            elif 'datetime' == fieldtype or 'timestamp' in fieldtype:
                df = df.withColumn(fieldname, F.col(fieldname).cast(TimestampType()))

            else:
                df = df.withColumn(fieldname, F.col(fieldname).cast(StringType())) \
                .withColumn(fieldname, F.regexp_replace(F.col(fieldname),"[\\r\\n]", '')) \
                .withColumn(fieldname, F.trim(F.col(fieldname)))

                if empty_columns_to_none:
                    df = df.withColumn(fieldname, F.when(F.col(fieldname)=="",None).otherwise(F.col(fieldname)))

        return df

def choose_last_row_modify_by_ids(df: DataFrame, list_columns_primary_key: list, list_columns_order: list, logger=None) -> DataFrame:
    logger = get_logger(logger)
    if (df is None or list_columns_primary_key is None or list_columns_order is None) or (len(list_columns_primary_key) == 0 or len(list_columns_order) == 0):
        raise ValueError('The values could by not null or empty')
    else:
        logger.info(f"Creating partitionBy {','.join(list_columns_primary_key)}")
        partition_order = Window.partitionBy(*list_columns_primary_key).orderBy(*[F.col(lc).desc() for lc in list_columns_order])
        logger.info("Filtering rn column")
        df_filter = df.withColumn('rn', F.row_number().over(partition_order)).filter('rn = 1').drop('rn')
        return df_filter

def rename_columns_by_list(df: DataFrame, list_columns: List, logger=None) -> DataFrame:
    logger = get_logger(logger)
    for rw in list_columns:
        if rw['column_name'] in df.columns:
            logger.info(f"renaming column: {rw['column_name']} to {rw['new_name']}")
            df = df.withColumnRenamed(rw['column_name'], rw['new_name'])
    return df

def format_df(df: DataFrame, schema: Dict) -> DataFrame:
    columns_df = [s['column_name'] for s in schema['columns']]
    df = df.select(*columns_df)
    df = cast_columns_types_by_schema(df, schema['columns'], True)
    return df

def drop_columns_by_list(df: DataFrame, list_columns: list, logger=None) -> DataFrame:
    logger = get_logger(logger)
    for df_col in [df_col for df_col in df.columns if df_col not in list_columns]:
        logger.info(f"drop column: {df_col}")
        df = df.drop(F.col(df_col))
    return df

def add_columns_by_list(df: DataFrame, list_columns: list, logger=None) -> DataFrame:
    logger = get_logger(logger)
    for df_col in [df_col for df_col in list_columns if df_col not in df.columns]:
        logger.info(f"add column: {df_col}")
        df = df.withColumn(df_col, F.lit(None))
    return df