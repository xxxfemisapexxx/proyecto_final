from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(   
    name = f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.clientes_raw",
    comment="Entidad maestra con la información de los clientes de la tienda"
    )
def clientes_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")        
        .option("cloudFiles.schemaLocation", "/Volumes/proyecto_final_dmc/landing/raw_data/_schemas/clientes")
        .load("/Volumes/proyecto_final_dmc/landing/raw_data/clientes")        
        )
