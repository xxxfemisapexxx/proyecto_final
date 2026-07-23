CREATE OR REPLACE MATERIALIZED VIEW proyecto_final_dmc.${gold_chema}.dim_tiempo
COMMENT "Dimension de tiempo, con columnas derivadas para mayor analisis"
TBLPROPERTIES ('quality' = 'gold')
AS
WITH rango_fechas AS (
  SELECT 
    EXPLODE(
      SEQUENCE(
        DATE '2020-01-01', 
        DATE '2030-12-31', 
        INTERVAL 1 DAY
      )
    ) AS fecha
)
SELECT
  CAST(DATE_FORMAT(fecha, 'yyyyMMdd') AS INT) AS id_tiempo,
  fecha,
  YEAR(fecha)                                  AS anio,
  MONTH(fecha)                                 AS num_mes,
  DATE_FORMAT(fecha, 'MMMM')                   AS nombre_mes,
  DAYOFMONTH(fecha)                            AS num_dia_mes,
  DATE_FORMAT(fecha, 'EEEE')                   AS nombre_dia,
  WEEKDAY(fecha) + 1                           AS num_dia_semana
FROM rango_fechas;