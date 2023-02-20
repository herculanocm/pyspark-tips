import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType, DateType, TimestampType, IntegerType, BooleanType

def cast_columns_types_by_schema(df: DataFrame, list_schema: list, empty_columns_to_none: bool = False) -> DataFrame:
    for column in list_schema:
        fieldname = column['column_name']
        fieldtype = column['data_type'].lower()

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
        df=df.select([F.when(F.col(c)=="",None).otherwise(F.col(c)).alias(c) for c in df.columns])

    return df