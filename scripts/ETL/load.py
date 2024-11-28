
from pyspark.sql.dataframe import DataFrame


def load_to_hdfs(df:DataFrame, output_path:str)->None:
    # df.write.format("csv").mode("overwrite").save(output_path, header=True)
    df.write.mode("overwrite").save(output_path)
    


    
