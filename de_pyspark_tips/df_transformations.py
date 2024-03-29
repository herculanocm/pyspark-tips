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

 
@F.udf(returnType=DoubleType()) 
def format_string_to_double_v1(value_decimal, return_none_if_except=True):
    try:
        str_value_decimal = str(value_decimal).strip().replace(' ','')
        
        
    
        if str_value_decimal.find('.') > 0 and str_value_decimal.find(',') > 0:
            return float((str_value_decimal.replace('.','')).replace(',','.'))
        elif str_value_decimal.find(',') > 0:
            return float(str_value_decimal.replace(',','.'))
        else:
            return float(str_value_decimal)
    except:
        if return_none_if_except == True:
            return None
        else:
            return float(0)


@F.udf(returnType=StringType())
def check_date_format_v1(date_string, format="%Y-%m-%d"):

    if date_string is None:
        return None

    from datetime import datetime

    valid = False
    try:
        datetime_converted = datetime.strptime(date_string, format)
        valid = True
    except ValueError:
        valid = False

    datetime_compare = datetime(1900, 1, 1)
    if valid and datetime_converted > datetime_compare:
        return datetime_converted.date().strftime(format)
    else:
        return datetime_compare.strftime(format)


@F.udf(returnType=StringType())
def check_datetime_format_v1(date_string, format='%Y-%m-%d %H:%M:%S.%f'):

    if date_string is None:
        return None

    from datetime import datetime

    valid = False
    try:
        datetime_converted = datetime.strptime(date_string, format)
        valid = True
    except ValueError:
        valid = False

    if valid == False:
        format_2 = '%Y-%m-%d %H:%M:%S'
        try:
            datetime_converted = datetime.strptime(date_string, format_2)
            valid = True
        except ValueError:
            valid = False

    datetime_compare = datetime(1900, 1, 1, 23, 59, 59, 999999)
    if valid and datetime_converted > datetime_compare:
        return datetime_converted.strftime(format)
    else:
        return datetime_compare.strftime(format)


def cast_columns_types_by_schema(df: DataFrame, list_schema: list, empty_columns_to_none: bool = False, truncate_string: bool = True, truncate_string_length: int = 16382, logger=None) -> DataFrame:
    logger = get_logger(logger)
    if (df is None or list_schema is None) or (len(list_schema) == 0):
        raise ValueError('The values could by not null or empty')
    else:
        for column in list_schema:
            fieldname = column['column_name']
            fieldtype = column['data_type'].lower()

            # logger.info(
            #     f"Casting column : {column['column_name']} to data_type: {column['data_type']}")

            if 'int' in fieldtype and 'big' not in fieldtype:
                df = df.withColumn(fieldname, F.col(
                    fieldname).cast(IntegerType()))

            elif 'bool' in fieldtype:
                df = df.withColumn(fieldname, F.col(
                    fieldname).cast(BooleanType()))

            elif 'numeric' in fieldtype or 'decimal' in fieldtype or \
                'double' in fieldtype or 'float' in fieldtype or \
                'real' in fieldtype or 'money' in fieldtype or \
                    'currency' in fieldtype:
                df = df.withColumn(fieldname, F.col(fieldname).cast(DoubleType()))

            elif 'date' == fieldtype:
                df = df.withColumn(fieldname, F.col(fieldname).cast(DateType()))

            elif 'datetime' == fieldtype or 'timestamp' in fieldtype:
                df = df.withColumn(fieldname, F.col(fieldname).cast(TimestampType()))

            else:
                df = df.withColumn(fieldname, F.col(fieldname).cast(StringType())).withColumn(fieldname, F.trim(F.col(fieldname)))
                
                #.withColumn(fieldname, F.regexp_replace(F.col(fieldname), "[\\r\\n]", '')) \

                if truncate_string == True:
                    df = df.withColumn(fieldname, F.substring(fieldname, 1,truncate_string_length))

                if empty_columns_to_none:
                    df = df.withColumn(fieldname, F.when(
                        F.col(fieldname) == "", None).otherwise(F.col(fieldname)))

        return df
    
def get_df_with_jsons_v1(df: DataFrame, json_columns: list, logger=None) -> DataFrame:
    
    for jcol in json_columns:
        for jdcol in jcol['column_destinations']:
             
            if 'int' in jdcol['data_type'] and 'big' not in jdcol['data_type']:
                df = df.withColumn(jdcol['alias_name'], F.get_json_object(F.col(jcol['column_origem']), jdcol['path_key'])) \
                        .withColumn(jdcol['alias_name'], F.col(jdcol['alias_name']).cast(IntegerType()))
                
            elif 'bool' in jdcol['data_type']:
                df = df.withColumn(jdcol['alias_name'], F.get_json_object(F.col(jcol['column_origem']), jdcol['path_key'])) \
                        .withColumn(jdcol['alias_name'], F.col(jdcol['alias_name']).cast(BooleanType()))
                    
                
            elif 'numeric' in jdcol['data_type'] or 'decimal' in jdcol['data_type'] or \
                'double' in jdcol['data_type'] or 'float' in jdcol['data_type'] or \
                'real' in jdcol['data_type'] or 'money' in jdcol['data_type'] or \
                'currency' in jdcol['data_type']:
                    
                df = df.withColumn(jdcol['alias_name'], F.get_json_object(F.col(jcol['column_origem']), jdcol['path_key'])) \
                        .withColumn(jdcol['alias_name'], F.col(jdcol['alias_name']).cast(DoubleType()))
                    
  
            elif 'date' == jdcol['data_type']:
                
                df = df.withColumn(jdcol['alias_name'], F.get_json_object(F.col(jcol['column_origem']), jdcol['path_key'])) \
                        .withColumn(jdcol['alias_name'], F.col(jdcol['alias_name']).cast(DateType()))
                
            elif 'date' in jdcol['data_type'] or 'timestamp' in jdcol['data_type']:
                
                df = df.withColumn(jdcol['alias_name'], F.get_json_object(F.col(jcol['column_origem']), jdcol['path_key'])) \
                        .withColumn(jdcol['alias_name'], F.col(jdcol['alias_name']).cast(TimestampType()))
                        
            else:
                df = df.withColumn(jdcol['alias_name'], F.get_json_object(F.col(jcol['column_origem']), jdcol['path_key'])) \
                        .withColumn(jdcol['alias_name'], F.col(jdcol['alias_name']).cast(StringType())) \
                        .withColumn(jdcol['alias_name'], F.regexp_replace(F.col(jdcol['alias_name']), "[\\r\\n]", '')) \
                        .withColumn(jdcol['alias_name'], F.trim(F.col(jdcol['alias_name'])))
                        
    return df

def choose_last_row_modify_by_ids(df: DataFrame, list_columns_primary_key: list, list_columns_order: list, logger=None) -> DataFrame:
    logger = get_logger(logger)
    if (df is None or list_columns_primary_key is None or list_columns_order is None) or (len(list_columns_primary_key) == 0 or len(list_columns_order) == 0):
        raise ValueError('The values could by not null or empty')
    else:
        logger.info(
            f"Creating partitionBy {','.join(list_columns_primary_key)}")
        partition_order = Window.partitionBy(
            *list_columns_primary_key).orderBy(*[F.col(lc).desc() for lc in list_columns_order])
        logger.info("Filtering rn column")
        df_filter = df.withColumn('rn', F.row_number().over(
            partition_order)).filter('rn = 1').drop('rn')
        return df_filter


def rename_columns_by_list(df: DataFrame, list_columns: List, logger=None) -> DataFrame:
    logger = get_logger(logger)
    for rw in list_columns:
        if rw['column_name'] in df.columns:
            logger.info(
                f"renaming column: {rw['column_name']} to {rw['new_name']}")
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
