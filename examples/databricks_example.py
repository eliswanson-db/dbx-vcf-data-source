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
%pip install vcf-spark-reader

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
spark.dataSource.register(VCFDataSource)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Batch Read Single VCF File

# COMMAND ----------

# Read a single VCF file
vcf_path = "/path/to/your/file.vcf"
df = spark.read.format("vcf").load(vcf_path)

# Show schema (includes file_path, file_name, and variant_id columns)
df.printSchema()

# Show first few rows
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1b: Read Directory of VCF Files

# COMMAND ----------

# Read all VCF files in a directory (recursively)
vcf_directory = "/path/to/your/vcf_directory"
df_dir = spark.read.format("vcf").load(vcf_directory)

# Show file metadata
display(df_dir.select("file_name", "file_path", "contig", "start", "end"))

# Count variants per file
display(df_dir.groupBy("file_name").count().orderBy("file_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1c: Generate Compound Primary Key

# COMMAND ----------

# Read with primary key generation enabled
df_with_pk = spark.read.format("vcf") \
    .option("generatePrimaryKey", "true") \
    .load(vcf_directory)

# Show variant ID (SHA256 hash of file_path|file_name|contig|start|end)
display(df_with_pk.select("variant_id", "file_name", "contig", "start", "end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1d: Exclude File Metadata

# COMMAND ----------

# Read without file metadata columns
df_no_metadata = spark.read.format("vcf") \
    .option("includeFileMetadata", "false") \
    .load(vcf_path)

# File metadata columns will be empty strings
display(df_no_metadata.select("contig", "start", "file_path", "file_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Query Variants in a Genomic Region

# COMMAND ----------

# Filter by chromosome and position
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

# Extract specific INFO fields
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

# Explode genotypes to one row per sample
genotypes_df = df.select(
    "contig",
    "start",
    "referenceAllele",
    "alternateAlleles",
    explode("genotypes").alias("genotype")
)

# Extract genotype information
genotype_details = genotypes_df.select(
    "contig",
    "start",
    col("genotype.sampleId"),
    col("genotype.calls"),
    col("genotype.data:DP").alias("read_depth")
)

display(genotype_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Filter Specific Samples

# COMMAND ----------

# Read only specific samples
filtered_df = spark.read.format("vcf") \
    .option("includeSampleIds", "Sample1,Sample2") \
    .load(vcf_path)

display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Write to Delta Table

# COMMAND ----------

# Write VCF data to Delta table for downstream analysis
df.write.format("delta").mode("overwrite").saveAsTable("variants_bronze")

# COMMAND ----------

# Query the Delta table
spark.sql("""
    SELECT contig, start, end, referenceAllele, alternateAlleles, qual
    FROM variants_bronze
    WHERE qual > 30
    ORDER BY contig, start
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Streaming Read (Single File or Directory)

# COMMAND ----------

# For streaming, set up a checkpoint location
checkpoint_path = "/tmp/vcf_stream_checkpoint"

# Read VCF as a stream (works with single file or directory)
stream_df = spark.readStream.format("vcf").load(vcf_path)

# Can also stream from directory with primary key generation
# stream_df = spark.readStream.format("vcf") \
#     .option("generatePrimaryKey", "true") \
#     .load(vcf_directory)

# Write to Delta table with streaming
query = stream_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .table("variants_streaming")

# COMMAND ----------

# Check streaming query status
query.status

# COMMAND ----------

# Stop the streaming query when done
query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 8: Aggregation - Variant Counts by Chromosome

# COMMAND ----------

from pyspark.sql.functions import count

variant_counts = df.groupBy("contig").agg(
    count("*").alias("variant_count")
).orderBy("contig")

display(variant_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 9: Filter by Quality and Coverage

# COMMAND ----------

from pyspark.sql.functions import col

high_quality_variants = df.filter(
    (col("qual") > 30) &
    (col("info:DP") > 20)
).select("contig", "start", "referenceAllele", "alternateAlleles", "qual", col("info:DP").alias("depth"))

display(high_quality_variants)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 10: Multi-Sample Analysis

# COMMAND ----------

from pyspark.sql.functions import explode, size

# Count samples per variant
sample_counts = df.withColumn("sample_count", size("genotypes"))

# Get variants with heterozygous calls
het_variants = genotypes_df.filter(
    (col("genotype.calls")[0] != col("genotype.calls")[1]) &
    (col("genotype.calls")[0] >= 0) &
    (col("genotype.calls")[1] >= 0)
)

display(het_variants.select("contig", "start", col("genotype.sampleId"), col("genotype.calls")))

