from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(   
    name = f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.pedidos_raw",
    comment="Cabecera de cada pedido realizado."
    )
def pedidos_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "JSON")
        .option("multiline", "true")        
        .option("cloudFiles.schemaLocation", "/Volumes/proyecto_final_dmc/landing/raw_data/_schemas/pedidos")
        .load("/Volumes/proyecto_final_dmc/landing/raw_data/pedidos")              
        )
