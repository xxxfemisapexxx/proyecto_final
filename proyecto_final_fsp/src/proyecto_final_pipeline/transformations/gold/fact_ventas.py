from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.expect_or_fail("cantidad_no_nula", "cantidad IS NOT NULL and cantidad > 0")
@dp.expect_or_fail("precio_unitario_no_nulo", "precio_unitario IS NOT NULL and precio_unitario > 0")
@dp.expect_or_fail("descuento_no_nulo", "descuento IS NOT NULL")
@dp.expect_or_fail("descuento_no_negativo", "descuento >= 0 and descuento <= 1.00")
@dp.expect_or_fail("monto_total_no_negativo", "monto_total >= 0")
@dp.table(
    name=f"proyecto_final_dmc.{spark.conf.get('gold_schema')}.fact_ventas",
    comment="Tabla de Hechos, consolidada de tablas silver y dimension tiempo",
    table_properties={"quality": "gold"}
)
def fact_detalle_pedidos():
    
    df_detalle_pedidos = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.detalle_pedidos")
    df_pedidos = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.pedidos")
    df_clientes = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('gold_schema')}.dim_clientes")
    df_dim_tiempo = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('gold_schema')}.dim_tiempo")
    df_productos = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('gold_schema')}.dim_productos")

    df_consolidado = (
        df_detalle_pedidos.alias("dp")
        .join(
            df_pedidos.alias("p"), 
            F.col("dp.order_id") == F.col("p.order_id"), 
            "left"
        )
        .join(
            F.broadcast(df_clientes).alias("c"), 
            F.col("p.customer_id") == F.col("c.customer_id"), 
            "left"
        )
        .join(
            F.broadcast(df_dim_tiempo).alias("t"), 
            F.col("p.id_tiempo") == F.col("t.id_tiempo"), 
            "left"
        )
        .join(
            F.broadcast(df_productos).alias("pr"), 
            F.col("dp.product_id") == F.col("pr.product_id"), 
            "left"
        )
    )

    df_resultado = df_consolidado.select(
        F.col("dp.order_item_id"),
        F.col("p.order_id"),
        F.col("c.customer_id").alias("customer_key"),
        #F.col("c.nombre").alias("nombre_cliente"),
        #F.col("c.apellido").alias("apellido_cliente"),
        #F.col("c.segmento"),
        F.col("pr.product_id").alias("product_key"),
        #F.col("pr.categoria"),
        #F.col("pr.nombre_producto"),
        #F.col("pr.subcategoria"),
        #F.col("pr.proveedor"),
        #F.col("p.canal_venta"),
        #F.col("p.fecha_pedido"),
        F.col("t.id_tiempo").alias("date_key"), 
        F.col("dp.cantidad").alias("cantidad"),
        F.col("dp.precio_unitario").alias("precio"),
        F.col("dp.descuento").alias("descuento"),
        #F.col("p.total_pedido"),
        (
            F.coalesce(F.col("dp.cantidad"), F.lit(0)) * F.coalesce(F.col("dp.precio_unitario"), F.lit(0)) *  (F.lit(1.00) - F.coalesce(F.col("dp.descuento"), F.lit(0)))
        ).cast("decimal(10,2)").alias("monto_total")
    )

    return df_resultado