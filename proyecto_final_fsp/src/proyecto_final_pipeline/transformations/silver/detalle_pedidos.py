from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,DateType, DecimalType


@dp.expect_or_fail("order_item_id_no_nulo ", "order_item_id IS NOT NULL")
@dp.expect_or_fail("order_id_no_nulo ", "order_id IS NOT NULL")
@dp.expect_or_fail("product_id _no_nulo ", "product_id IS NOT NULL")
@dp.expect_or_fail("cantidad_mayor_cero ", "cantidad > 0 ")
@dp.view(name="vw_detalle_pedidos_preparados") 


def detalle_pedidos():    
    df_silver_detalle_pedidos = spark.readStream.table(f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.detalle_pedidos_raw")    
    
    return df_silver_detalle_pedidos.selectExpr(
        "CAST(order_item_id AS INT) AS order_item_id",
        "CAST(order_id AS INT) AS order_id",
        "CAST(product_id AS INT) AS product_id",
        "CAST(cantidad AS INT) AS cantidad",
        "CAST(precio_unitario AS DECIMAL(10,2)) AS precio_unitario",
        "CAST(descuento AS DECIMAL(10,2)) AS descuento",
        "CAST(audit_timestamp AS TIMESTAMP) AS audit_timestamp"
    )

dp.create_streaming_table(   
    name = f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.detalle_pedidos",
    comment="Cabecera de cada pedido realizado.",
    table_properties={"quality": "silver"}     
    )


dp.create_auto_cdc_flow(
    name='cdc_detalle_pedidos_silver',      
    target=f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.detalle_pedidos",
    source="LIVE.vw_detalle_pedidos_preparados", 
    keys=["order_item_id"],
    sequence_by="audit_timestamp",
    stored_as_scd_type="1"
)