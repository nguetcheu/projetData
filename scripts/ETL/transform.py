from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType,  NumericType
from pyspark.sql.functions import when, col, date_format, from_unixtime, unix_timestamp, trim, isnan, isnull, lit


def format_columns_names(df:DataFrame):
    df=df.toDF(*[_.replace(".", "_") for _ in df.columns])
    return df


def set_default_data(df:DataFrame)->DataFrame:
    #définir les valeurs vides

    for _ in df.schema.fields:
        if  isinstance(_.dataType, StringType):
            df=df.withColumn(_.name, trim(col(_.name)))
            df=df.withColumn(_.name, when(isnull(col(_.name)), "___empty___").otherwise(col(_.name)))
        elif issubclass(type(_.dataType),NumericType):
            df=df.withColumn(_.name, when(isnan(col(_.name)) | isnull(col(_.name)), 0.0).otherwise(col(_.name)))

    df=df.withColumn("moonrise", when(col("moonrise")=="No moonrise", None).otherwise(col("moonrise")))
    df=df.withColumn("moonset", when(col("moonset")=="No moonset", None).otherwise(col("moonset")))
    
    return df

    



def transform_date_format(df:DataFrame)->DataFrame:
    #définit les formats de date

    df=df.withColumn("last_updated", date_format(col("last_updated"), "dd-MM-yyy HH:mm"))
    
    df=df.withColumn("sunrise_24h", from_unixtime(unix_timestamp(col("sunrise"), "hh:mm a"), "HH:mm"))
    df=df.withColumn("sunset_24h", from_unixtime(unix_timestamp(col("sunset"), "hh:mm a"), "HH:mm"))
    df=df.withColumn("moonrise_24h", from_unixtime(unix_timestamp(col("moonrise"), "hh:mm a"), "HH:mm"))
    df=df.withColumn("moonset_24h", from_unixtime(unix_timestamp(col("moonset"), "hh:mm a"), "HH:mm"))
    return df





