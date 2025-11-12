# Databricks notebook source
# MAGIC %md
# MAGIC # VCF Spark Reader - Databricks Example
# MAGIC 
# MAGIC This notebook demonstrates how to use the VCF Spark Reader in Databricks.
# MAGIC 
# MAGIC **Requirements**: Databricks Runtime 15.4 LTS or above

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation

# COMMAND ----------

# Install the VCF reader
# Specify the volume or location where your wheel has been uploaded.
%pip install https://github.com/eliswanson-db/dbx-vcf-data-source/edit/main

# COMMAND ----------

# Restart Python to load the new package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from vcf_reader import VCFDataSource
from pyspark.sql import SparkSession

# Register the data source
### Is there a way to do this so it doesn't have to be specified in a notebook
spark.dataSource.register(VCFDataSource)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Batch Read Single VCF File

# COMMAND ----------

dbutils.widgets.text("file_vcf_path", "", "Path to single file")
dbutils.widgets.text("directory_vcf_path", "", "Path to directory with multiple files")
file_vcf_path = dbutils.widgets.get("file_vcf_path")
directory_vcf_path = dbutils.widgets.get("directory_vcf_path")


# COMMAND ----------

df = spark.read.format("vcf").load(file_vcf_path)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1b: Read Directory of VCF Files

# COMMAND ----------

df_dir = spark.read.format("vcf").load(directory_vcf_path)

# Show file metadata
display(df_dir.select("file_name", "file_path", "contig", "start", "end"))

# Count variants per file
display(df_dir.groupBy("file_name").count().orderBy("file_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1c: Generate Compound Primary Key

# COMMAND ----------

df_with_pk = spark.read.format("vcf") \
    .option("generatePrimaryKey", "true") \
    .load(vcf_directory)

display(df_with_pk.select("variant_id", "file_name", "contig", "start", "end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1d: Exclude File Metadata

# COMMAND ----------

df_no_metadata = spark.read.format("vcf") \
    .option("includeFileMetadata", "false") \
    .load(vcf_path)

# No metadata should show
display(df_no_metadata.select("contig", "start", "file_path", "file_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Query Variants in a Genomic Region

# COMMAND ----------

region_df = df.filter(
    (df.contig == "chr1") & 
    (df.start >= 1000000) & 
    (df.end <= 2000000)
)

display(region_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Access INFO Fields

# COMMAND ----------

from pyspark.sql.functions import col

info_df = df.select(
    "contig",
    "start",
    "referenceAllele",
    "alternateAlleles",
    col("info:DP").alias("depth"),
    col("info:AF").alias("allele_frequency")
)

display(info_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Work with Genotypes

# COMMAND ----------

from pyspark.sql.functions import explode

genotypes_df = df.select(
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
    col("genotype.data:DP").alias("read_depth")
)

display(genotype_details)
