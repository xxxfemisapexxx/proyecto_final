from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DecimalType

@dp.expect_or_fail("product_id_no_nulo", "product_id IS NOT NULL")
@dp.expect_or_fail("precio_mayor_a_cero", "precio_unitario > 0")
@dp.expect_or_fail("stock_no_negativo", "stock_actual >= 0")
@dp.view(name="vw_productos_preparados") 
def productos_silver():
    df_silver_productos = spark.readStream.table(f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.productos_raw")
    
    return df_silver_productos.selectExpr(
        "CAST(product_id AS INT) AS product_id",
        "CAST(nombre_producto AS STRING) AS nombre_producto",
        "CAST(categoria AS STRING) AS categoria",
        "CAST(subcategoria AS STRING) AS subcategoria",
        "CAST(precio_unitario AS DECIMAL(10,2)) AS precio_unitario",
        "CAST(proveedor AS STRING) AS proveedor",        
        "CAST(stock_actual AS INT) AS stock_actual",
        "CAST(audit_timestamp AS TIMESTAMP) AS audit_timestamp"
    )


dp.create_streaming_table(   
    name = f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.productos",
    comment="Catálogo de productos vendidos.",
    table_properties={"quality": "silver"}     
    )


dp.create_auto_cdc_flow(
    name='cdc_productos_silver',      
    target=f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.productos",
    source="LIVE.vw_productos_preparados", 
    keys=["product_id"],
    sequence_by="audit_timestamp",
    stored_as_scd_type="1"
)
