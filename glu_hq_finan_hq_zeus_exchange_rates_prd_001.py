#%help

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.types import DateType
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
bucket = "s3-hq-std-prd-finan"
prefix = "xchng_rts"
temp = "tmp_xchng_rts"
bucket_path = "s3a://{}/{}/".format(bucket,prefix)
temp_bucket_path = "s3a://{}/{}/".format(bucket,temp)


session = boto3.Session()
s3 = session.client('s3')
def path_exists(buck, path):
    response = s3.list_objects_v2(Bucket=buck, Prefix=path)
    return 'Contents' in response

# VALORES MANUALES DEL BUDGET DE 2023 -- NO EXISTEN EN NINGUNA FUENTE SOLO SON ANUNCIADOS POR EL DEPARTAMENTO
rows = [
    Row(FROM_CURRENCY="USD", TO_CURRENCY="BOB", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023",  RATE=6.91, CNTRY_CD="BO"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="GTQ", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023", RATE=7.75, CNTRY_CD="GT"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="HNL", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023", RATE=24.85, CNTRY_CD="HN"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="COP", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023",  RATE=4550.00, CNTRY_CD="CO"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="PYG", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023", RATE=7150.00, CNTRY_CD="PY"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="USD", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023",  RATE=1.00, CNTRY_CD="PA"), 
    Row(FROM_CURRENCY="USD", TO_CURRENCY="USD", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023",  RATE=1.00, CNTRY_CD="SV"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="NIO", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023", RATE=36.60, CNTRY_CD="NI"),
    Row(FROM_CURRENCY="USD", TO_CURRENCY="CRC", ORIGIN_CONVERSION_TYPE="MANUAL 2023", EXCH_RATE_TYPE="BUD 2023", EXCH_DATE="31/12/2023", RATE=670.00, CNTRY_CD="CR")
]
budget = spark.createDataFrame(rows)
budget = budget.withColumn("EXCH_DATE", to_date(col("EXCH_DATE"), 'dd/MM/yyyy'))

#read daily rate
daily_rates = glueContext.create_dynamic_frame.from_catalog(database='hq-std-prd-finan-link', table_name='daily_rates').toDF()

#unidades de conversion, agregar pais conforme la necesidad
daily_rates_filtered = daily_rates.filter(
    (col("from_currency") == lit("USD")) &
    (
        ((col("to_currency") == lit("BOB")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) | #BO Oficial  #HQ End Of Month #FOREX - Average
        ((col("to_currency") == lit("GTQ")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) | #GT Oficial #HQ End Of Month #FOREX - Average
        ((col("to_currency") == lit("HNL")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) |    #HN Venta #HQ End Of Month #FOREX - Average
        ((col("to_currency") == lit("COP")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) | 
        ((col("to_currency") == lit("PYG")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) | 
        ((col("to_currency") == lit("USD")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) |    
        ((col("to_currency") == lit("NIO")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) |
        ((col("to_currency") == lit("CRC")) & (col("user_conversion_type").isin(lit("HQ End Of Month")))) 
    )
)

# AGREGANDO CAMPO CNTRY_CD
daily_rates_filtered1 = daily_rates_filtered.withColumn(
    "CNTRY_CD",
    when(
        col("to_currency") == lit("BOB"),
        lit("BO")
    ).when(
        col("to_currency") == lit("GTQ"),
        lit("GT")
    ).when(
        col("to_currency") == lit("HNL"),
        lit("HN")
    ).when(
        col("to_currency") == lit("COP"),
        lit("CO")
    ).when(
        col("to_currency") == lit("PYG"),
        lit("PY")
    ).when(
        col("to_currency") == lit("NIO"),
        lit("NI")
    ).when(
        col("to_currency") == lit("CRC"),
        lit("CR")
    ).otherwise(None)
)

daily_rates_filtered1 = daily_rates_filtered1.filter(col("CNTRY_CD").isNotNull())
#daily_rates_filtered.count()
# Agregar lógica para manejar el caso de 'USD' en SV y PA
daily_rates_filtered2 = daily_rates_filtered.withColumn(
    "CNTRY_CD",
    when(col("to_currency") == lit("USD"), array(lit("SV"), lit("PA"))).otherwise(None)
)
# Explode the array to create multiple rows
daily_rates_filtered2 = daily_rates_filtered2.withColumn("CNTRY_CD", explode(col("CNTRY_CD")))

# Filtrar para eliminar las filas con CNTRY_CD nulo si es necesario
daily_rates_filtered2 = daily_rates_filtered2.filter(col("CNTRY_CD").isNotNull())

# Por un lado se procesan SV y PA ya que manejan la misma moneda de USD y por otro lado el resto de los paises que se mapean con la moneda
daily_rates_filtered3 = daily_rates_filtered2.union(daily_rates_filtered1)

#limpiando todos los exchanges rates de daily rates
exchange_rates = daily_rates_filtered3.select(
    col("FROM_CURRENCY").alias("FROM_CURRENCY"),
    col("TO_CURRENCY").alias("TO_CURRENCY"),
    col("user_conversion_type").alias("ORIGIN_CONVERSION_TYPE"),
    lit("MONTHLY-AVG").alias("EXCH_RATE_TYPE"),
    col("conversion_date").alias("EXCH_DATE"),
    col("conversion_rate").alias("RATE"),
    col("CNTRY_CD").alias("CNTRY_CD")
)
# union de exchange rate y budget
#xchng_bdgt = exchange_rates.union(budget)
#xchng_bdgt.write.mode("overwrite").partitionBy("EXCH_DATE","ORIGIN_CONVERSION_TYPE").parquet(bucket_path)
#spark.read.parquet(bucket_path).show()
#daily_rates_filtered.count()
#daily_rates_filtered.filter((col("CONVERSION_DATE")>=lit("2023-09-01")) & (col("from_currency")==lit("USD")) & (col("to_currency")==lit("BOB")) & (col("user_conversion_type")==lit("HQ End Of Month"))).show()

#fecha maxima de conversion
max_conversion_date = exchange_rates.groupBy("from_currency", "to_currency").agg(max(col("EXCH_DATE")).alias("latest_date"), max(col("ORIGIN_CONVERSION_TYPE")).alias("ORIGIN_CONVERSION_TYPE"))
#max_conversion_date.show()

#join con max_conversion_date para obtener ultimo precio de conversion_rate
max_exchange_rate = max_conversion_date.alias("A").join(
    exchange_rates.alias("B"),
    (col("A.from_currency") == col("B.from_currency")) &
    (col("A.to_currency") == col("B.to_currency")) &
    (col("A.ORIGIN_CONVERSION_TYPE") == col("B.ORIGIN_CONVERSION_TYPE")) &
    (col("A.latest_date") == col("B.EXCH_DATE")),
    "leftouter"
).select(
    col("A.from_currency").alias("FROM_CURRENCY"),
    col("A.to_currency").alias("TO_CURRENCY"),
    col("A.ORIGIN_CONVERSION_TYPE").alias("ORIGIN_CONVERSION_TYPE"),
    lit("MONTHLY-AVG").alias("EXCH_RATE_TYPE"),
    col("A.latest_date").alias("EXCH_DATE"),
    col("B.RATE").alias("RATE"),
    col("B.CNTRY_CD").alias("CNTRY_CD")
)
#max_exchange_rate.show()
#max_exchange_rate.orderBy("TO_CURRENCY","EXCH_DATE").show(1000)

# union de exchange rate y budget
xchng_rt_bdgt = max_exchange_rate.union(budget)

#xchng_rt_bdgt.write.mode("overwrite").parquet(bucket_path)
if path_exists(bucket, prefix):
    # leer bucket donde existe data actualmente de los exchange rates
    current_exchange_rate = spark.read.parquet(bucket_path)
    
    # todos los que no estan
    joined_exchange = xchng_rt_bdgt.alias("A").join(current_exchange_rate.alias("B"), on=[
        col("A.FROM_CURRENCY") == col("B.FROM_CURRENCY"),
        col("A.TO_CURRENCY") == col("B.TO_CURRENCY"),
        col("A.EXCH_RATE_TYPE") == col("B.EXCH_RATE_TYPE"),
        col("A.EXCH_DATE") == col("B.EXCH_DATE"),
        col("A.ORIGIN_CONVERSION_TYPE") == col("B.ORIGIN_CONVERSION_TYPE"),
        col("A.CNTRY_CD") == col("B.CNTRY_CD")
    ], how="left_anti")
    
    # Sobreescribir el archivo en S3 en formato Parquet
    joined_exchange.write.mode("append").partitionBy("EXCH_DATE","ORIGIN_CONVERSION_TYPE").parquet(bucket_path)
    
else:
    xchng_rt_bdgt.write.mode("overwrite").parquet(bucket_path)

#joined_exchange.write.mode("overwrite").parquet(temp_bucket_path)
#exchange_rate = spark.read.parquet(temp_bucket_path)

#spark.read.parquet(bucket_path).show()
#current_exchange_rate.filter(col("EXCH_DATE")>=lit("2023-01-01")).show(172)
job.commit()