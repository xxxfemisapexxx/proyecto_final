from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(   
    name = f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.productos_raw",
    comment="Catálogo de productos vendidos."
    )
def productos_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")        
        .option("cloudFiles.schemaLocation", "/Volumes/proyecto_final_dmc/landing/raw_data/_schemas/productos")
        .load("/Volumes/proyecto_final_dmc/landing/raw_data/productos")        
        )
