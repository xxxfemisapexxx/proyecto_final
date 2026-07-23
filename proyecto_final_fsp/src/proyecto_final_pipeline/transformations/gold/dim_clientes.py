from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.expect_or_fail("customer_id_no_nulo", "customer_id IS NOT NULL")
@dp.expect_or_fail("pk_cliente_unico", "es_unico = 1")
@dp.view(name="v_dim_clientes_validado")
def v_dim_clientes_validado():
    df_silver_clientes = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.clientes")

    df_dim_clientes = df_silver_clientes.select(
        F.col("customer_id"),
        F.col("nombre"),
        F.col("apellido"),
        F.col("email"),     
        F.col("ciudad"),
        F.col("pais"),
        F.col("fecha_registro"),
        F.col("segmento").alias("segmento")
    )

    window_pk = Window.partitionBy("customer_id")
    return df_dim_clientes.withColumn("es_unico", F.count("customer_id").over(window_pk))


# 2. TABLA GOLD: Remueve 'es_unico' para que la tabla física quede limpia
@dp.table(
    name=f"proyecto_final_dmc.{spark.conf.get('gold_schema')}.dim_clientes",
    comment="Dimension Clientes",
    table_properties={'quality': 'gold'}
)
def dim_clientes():
    # Lee de la vista previa ya validada y descarta la columna auxiliar
    return spark.read.table("LIVE.v_dim_clientes_validado").drop("es_unico")