# coding: utf-8
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('teste').config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()

#OBTEM A LISTA DE ARQUIVOS DO DIRETÓRIO
pathWild = '/user/cloudera/efd/*.txt'
time_start = time.time()

efd_df = spark.read.format("com.databricks.spark.csv").options(header=False, inferSchema=True, delimiter="|").load(pathWild).cache()

readTime = time.gmtime(time.time() - time_start)
print "==================================================================="
print(time.strftime("TEMPO DE LEITURA DOS ARQUIVOS: %H horas, %M minutes, %S seconds", readTime))
print "==================================================================="

efd_df = efd_df.drop('_c0').persist()

time_start_emit = time.time()
#GERA EMIT 0000
efd_emit = efd_df[(efd_df['_c1'] == '0000')].cache()
efd_emit = efd_emit.select(
  col("_c1").alias("reg"),
  col("_c2").alias("cod_ver"),
  col("_c3").alias("cod_fin"),
  col("_c4").alias("dt_ini"),
  col("_c5").alias("dt_fin"),
  col("_c6").alias("nome"),
  col("_c7").alias("cnpj"),
  col("_c8").alias("cpf"),
  col("_c9").alias("uf"),
  col("_c10").alias("ie"),
  col("_c11").alias("cod_mun"),
  col("_c12").alias("im"),
  col("_c13").alias("suframa"),
  col("_c14").alias("ind_perfil"),
  col("_c15").alias("ind_ativ")).persist()
emitTime = time.gmtime(time.time() - time_start_emit)
print "==================================================================="
print(time.strftime("TEMPO EXPLOSÃO efd_emit: %H horas, %M minutes, %S seconds", emitTime))
print "==================================================================="

time_start_apur = time.time()
#OBTEM LISTA DE CNPJ
cnpj_value = efd_emit.select(col('cnpj')).cache()
#GERA APUR E110
efd_apur = efd_df[(efd_df['_c1'] == 'E110')].cache()
efd_apur = efd_apur.select(
  col("_c1").alias("reg"),
  col("_c2").alias("vl_tot_debitos"),
  col("_c3").alias("vl_aj_debitos"),
  col("_c4").alias("vl_tot_aj_debitos"),
  col("_c5").alias("vl_estornos_cred"),
  col("_c6").alias("vl_tot_creditos"),
  col("_c7").alias("vl_aj_creditos"),
  col("_c8").alias("vl_tot_aj_creditos"),
  col("_c9").alias("vl_estornos_deb"),
  col("_c10").alias("vl_sld_credor_ant"),
  col("_c11").alias("vl_sld_apurado"),
  col("_c12").alias("vl_tot_ded"),
  col("_c13").alias("vl_icms_recolher"),
  col("_c14").alias("vl_sld_credor_transportar"),
  col("_c15").alias("deb_esp")).persist()
efd_apur=efd_apur.withColumn('row_index', monotonically_increasing_id()).persist()
cnpj_value=cnpj_value.withColumn('row_index', monotonically_increasing_id()).persist()
efd_apur = efd_apur.join(cnpj_value, on=["row_index"]).sort("row_index").drop("row_index").persist()

apurTime = time.gmtime(time.time() - time_start_apur)
print "==================================================================="
print(time.strftime("TEMPO EXPLOSÃO efd_apur: %H horas, %M minutes, %S seconds", apurTime))
print "==================================================================="

if efd_df.filter(col('_c1').isin(['C100'])).count() > 0:
    time_start_nf = time.time()
    efd_temp = efd_df.filter((col('_c1').isin(['0000'])) | (col('_c1').isin(['C100'])) | (col('_c1').isin(['C170']))).withColumn('cnpj', when(col('_c1')=='0000', col('_c7'))).withColumn('row_index', monotonically_increasing_id()).cache()
    my_window = Window.partitionBy().orderBy("row_index")
    efd_temp = efd_temp.withColumn("cnpj", last('cnpj', True).over(my_window)).drop("row_index").persist()
    #GERA NF C100
    efd_nf = efd_temp.filter(col('_c1').isin(['C100'])).cache()
    efd_nf = efd_nf.select(
      col("_c1").alias("reg"),
      col("_c2").alias("ind_oper"),
      col("_c3").alias("ind_emit"),
      col("_c4").alias("cod_part"),
      col("_c5").alias("cod_mod"),
      col("_c6").alias("cod_sit"),
      col("_c7").alias("ser"),
      col("_c8").alias("num_doc"),
      col("_c9").alias("chv_nfe"),
      col("_c10").alias("dt_doc"),
      col("_c11").alias("dt_e_s"),
      col("_c12").alias("vl_doc"),
      col("_c13").alias("ind_pgto"),
      col("_c14").alias("vl_desc"),
      col("_c15").alias("vl_abat_nt"),
      col("_c16").alias("vl_merc"),
      col("cnpj")).persist()

    nfTime = time.gmtime(time.time() - time_start_nf)
    print "==================================================================="
    print(time.strftime("TEMPO EXPLOSÃO efd_nf: %H horas, %M minutes, %S seconds", nfTime))
    print "==================================================================="

if efd_df.filter(col('_c1').isin(['C170'])).count() > 0:
    time_start_item = time.time()
    efd_temp2 = efd_temp.filter((col('_c1').isin(['C100'])) | (col('_c1').isin(['C170']))).withColumn('chv_nfe', when(col('_c1')=='C100', col('_c9'))).withColumn('row_index', monotonically_increasing_id()).cache()
    my_window2 = Window.partitionBy().orderBy("row_index")
    efd_temp2 = efd_temp2.withColumn("chv_nfe", last('chv_nfe', True).over(my_window2)).drop("row_index").persist()
    #GERA ITEM C170
    efd_item = efd_temp2.filter(col('_c1').isin(['C170'])).cache()
    efd_item = efd_item.select(
      col("_c1").alias("reg"),
      col("_c2").alias("num_item"),
      col("_c3").alias("cod_item"),
      col("_c4").alias("descr_compl"),
      col("_c5").alias("qtd"),
      col("_c6").alias("unid"),
      col("_c7").alias("vl_item"),
      col("_c8").alias("vl_desc"),
      col("_c9").alias("ind_mov"),
      col("_c10").alias("cst_icms"),
      col("_c11").alias("cfop"),
      col("_c12").alias("cod_nat"),
      col("_c13").alias("vl_bc_icms"),
      col("_c14").alias("aliq_icms"),
      col("_c15").alias("vl_icms"),
      col("_c16").alias("vl_bc_icms_st"),
      col("cnpj"),
      col("chv_nfe")).persist()

    itemTime = time.gmtime(time.time() - time_start_item)
    print "==================================================================="
    print(time.strftime("TEMPO EXPLOSÃO efd_item: %H horas, %M minutes, %S seconds", itemTime))
    print "==================================================================="

totalTime = time.gmtime(time.time() - time_start)
print "==================================================================="
print(time.strftime("TEMPO TOTAL: %H horas, %M minutes, %S seconds", totalTime))
print "==================================================================="

#INSERE OS DATAFRAMES NAS TABELAS
efd_emit.createOrReplaceTempView("temptable1")
emit_table = spark.sql("select * from temptable1")
emit_table.repartition(2, 'uf','cod_mun').write.partitionBy('uf','cod_mun').mode('append').format('parquet').saveAsTable("efd_data.efd_emit")

efd_apur.createOrReplaceTempView("temptable2")
apur_table = spark.sql("select * from temptable2")
apur_table.repartition(2, 'cnpj').write.partitionBy('cnpj').mode('append').format('parquet').saveAsTable("efd_data.efd_apur")

efd_nf.createOrReplaceTempView("temptable3")
nf_table = spark.sql("select * from temptable3")
nf_table.repartition(2, 'cnpj').write.partitionBy('cnpj').mode('append').format('parquet').saveAsTable("efd_data.efd_nf")

efd_item.createOrReplaceTempView("temptable4")
item_table = spark.sql("select * from temptable4")
item_table.repartition(2, 'cnpj').write.partitionBy('cnpj').mode('append').format('parquet').saveAsTable("efd_data.efd_item")

#configurar número de executores e memória