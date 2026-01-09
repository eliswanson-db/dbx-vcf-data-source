# Databricks notebook source
# MAGIC %md
# MAGIC # VCF Spark Reader - Databricks Example
# MAGIC
# MAGIC This notebook demonstrates how to use the VCF Spark Reader in Databricks.
# MAGIC
# MAGIC **Requirements**: Databricks Runtime 17.3 LTS or above

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation

# COMMAND ----------

# MAGIC %pip install git+https://github.com/eliswanson-db/dbx-vcf-data-source@main
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from vcf_reader import VCFDataSource
from pyspark.sql import SparkSession

spark.dataSource.register(VCFDataSource)

# COMMAND ----------

dbutils.widgets.text("file_vcf_path", "", "Path to single file")
dbutils.widgets.text("directory_vcf_path", "", "Path to directory with multiple files")
dbutils.widgets.text("catalog_schema", "", "Catalog and schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Batch Read Single VCF File

# COMMAND ----------

file_vcf_path = dbutils.widgets.get("file_vcf_path")
directory_vcf_path = file_vcf_path if file_vcf_path != "" else dbutils.widgets.get("directory_vcf_path")
catalog_schema = dbutils.widgets.get("catalog_schema")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1b: Read Directory of VCF Files

# COMMAND ----------

print(directory_vcf_path) #/Volumes/mfeichtel_classic_ws_catalog/vcf_data_source/raw_data/small_files/sample.vcf
print(catalog_schema)

# COMMAND ----------

# @mfeichtel - what is this doing? 
df = spark.read.format("vcf").option("generatePrimaryKey", "true").load(directory_vcf_path)
df = df.repartition(16)
df.write.mode('overwrite').saveAsTable(f"{catalog_schema}.vcf_output")

# COMMAND ----------

df_read = spark.read.table(f"{catalog_schema}.vcf_output")
display(df_read)

# COMMAND ----------

display(df_read.groupBy("contig").count())

# COMMAND ----------

df_read.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1c: Generate Compound Primary Key

# COMMAND ----------

df_with_pk = spark.read.format("vcf") \
    .option("generatePrimaryKey", "true") \
    .load(directory_vcf_path)

display(df_with_pk.select("variant_id", "file_name", "contig", "start", "end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1d: Exclude File Metadata

# COMMAND ----------

df_no_metadata = spark.read.format("vcf") \
    .option("includeFileMetadata", "false") \
    .load(directory_vcf_path)

# No metadata should show
display(df_no_metadata.select("contig", "start", "file_path", "file_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Query Variants in a Genomic Region

# COMMAND ----------

region_df = df_read.filter(
    (df_read.contig == "22") & 
    (df_read.start >= 10000000) & 
    (df_read.end <= 20000000)
)

display(region_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Access INFO Fields

# COMMAND ----------

from pyspark.sql.functions import col, variant_get

info_df = df_read.select(
    "contig",
    "start",
    "referenceAllele",
    "alternateAlleles",
    variant_get(col("info"), "$.DP", "string").alias("depth"),
    variant_get(col("info"), "$.AF", "string").alias("allele_frequency")
)

display(info_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Work with Genotypes

# COMMAND ----------

from pyspark.sql.functions import explode

genotypes_df = df_read.select(
    "contig",
    "start",
    "referenceAllele",
    "alternateAlleles",
    explode("genotypes").alias("genotype")
)

genotype_details = genotypes_df.select(
    "contig",
    "start",
    col("genotype.sampleId"),
    col("genotype.calls"),
    variant_get(col("genotype.data"), "$.DP", "string").alias("read_depth"),
    "genotype"
)

display(genotype_details)
