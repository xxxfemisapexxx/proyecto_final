from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,DateType, DecimalType

@dp.expect_or_fail("order_id_no_nulo", "order_id IS NOT NULL")
@dp.expect_or_fail("estado_pedido_permitidos", "estado_pedido IN ('completado','en_proceso','cancelado')")
@dp.expect_or_fail("total_pedido_no_negativo", "total_pedido >= 0")
@dp.view(name="vw_pedidos_preparados") 

def pedidos_silver():
    df_silver_pedidos = spark.readStream.table(f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.pedidos_raw")

    
    return df_silver_pedidos.selectExpr(
        "CAST(order_id AS INT) AS order_id",
        "CAST(customer_id AS INT) AS customer_id",
        "CAST(fecha_pedido AS DATE) AS fecha_pedido",
        "CAST(canal_venta AS STRING) AS canal_venta",
        "CAST(estado_pedido AS STRING) AS estado_pedido",
        "CAST(total_pedido AS DECIMAL(10,2)) AS total_pedido",
        "CAST(audit_timestamp AS TIMESTAMP) AS audit_timestamp",
        "CAST(date_format(fecha_pedido, 'yyyyMMdd') AS INT) AS id_tiempo"                
    )

dp.create_streaming_table(   
    name = f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.pedidos",
    comment="Cabecera de cada pedido realizado.",
    table_properties={"quality": "silver"}     
    )


dp.create_auto_cdc_flow(
    name='cdc_pedidos_silver',      
    target=f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.pedidos",
    source="LIVE.vw_pedidos_preparados", 
    keys=["order_id"],
    sequence_by="audit_timestamp",
    stored_as_scd_type="1"
)

