from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(   
    name = f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.detalle_pedidos_raw",
    comment="Detalle línea a línea de cada pedido (grano de la futura tabla de hechos)."
    )
def detalle_pedidos_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "JSON")
        .option("multiline", "true")        
        .option("cloudFiles.schemaLocation", "/Volumes/proyecto_final_dmc/landing/raw_data/_schemas/detalle_pedidos")
        .load("/Volumes/proyecto_final_dmc/landing/raw_data/detalle_pedidos")              
        )
