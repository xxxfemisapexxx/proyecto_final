from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

@dp.expect_or_fail("customer_id_no_nulo", "customer_id IS NOT NULL")
@dp.expect_or_fail("email_formato_valido", f"email RLIKE '{EMAIL_REGEX}'")
@dp.expect_or_fail("segmento_permitido", "segmento IN ('Retail','Premium')")

@dp.view(name="vw_clientes_preparados")
def clientes_silver():
    df_silver_clientes = spark.readStream.table(f"proyecto_final_dmc.{spark.conf.get('bronze_schema')}.clientes_raw")

    return df_silver_clientes.selectExpr(
        "CAST(customer_id AS INT) AS customer_id",
        "CAST(nombre AS STRING) AS nombre",
        "CAST(apellido AS STRING) AS apellido",
        "CAST(email AS STRING) AS email",
        "CAST(ciudad AS STRING) AS ciudad",
        "CAST(pais AS STRING) AS pais",
        "CAST(CAST(fecha_registro AS DATE) AS DATE) AS fecha_registro",
        "CAST(segmento AS STRING) AS segmento",
        "CAST(audit_timestamp AS TIMESTAMP) AS audit_timestamp"
    )    

dp.create_streaming_table(   
    name=f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.clientes",
    comment="Entidad maestra con la información de los clientes de la tienda.",
    table_properties={"quality": "silver"}
)

dp.create_auto_cdc_flow(
    name='cdc_clientes_silver',      
    target=f"proyecto_final_dmc.{spark.conf.get('silver_schema')}.clientes",
    source="LIVE.vw_clientes_preparados",
    keys=["customer_id"],
    sequence_by="audit_timestamp",
    stored_as_scd_type="1"
)