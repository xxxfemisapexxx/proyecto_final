from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.expect_or_fail("product_id_no_nulo", "product_id IS NOT NULL")
@dp.expect_or_fail("precio_mayor_a_cero", "precio_unitario > 0")
@dp.expect_or_fail("stock_no_negativo", "stock_actual >= 0")
@dp.view(name="v_dim_productos_validado")
def v_dim_productos_validado():
    df_silver_clientes = spark.read.table(f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.productos")

    df_dim_clientes = df_silver_clientes.select(
        F.col("product_id"),
        F.col("nombre_producto"),
        F.col("categoria"),
        F.col("subcategoria"),     
        F.col("precio_unitario"),
        F.col("proveedor"),
        F.col("stock_actual")
        
    )

    window_pk = Window.partitionBy("product_id")
    return df_dim_clientes.withColumn("es_unico", F.count("product_id").over(window_pk))

@dp.table(
    name=f"proyecto_final_dmc.{spark.conf.get('gold_schema')}.dim_productos",
    comment="Dimension Productos",
    table_properties={'quality': 'gold'}
)
def dim_clientes():    
    return spark.read.table("LIVE.v_dim_productos_validado").drop("es_unico")