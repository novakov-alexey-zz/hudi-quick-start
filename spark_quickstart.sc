// scala 2.12.11

import scala.util.Properties

pprint.log(Properties.releaseVersion)

import $ivy.`sh.almond:almond-spark_2.12:0.10.9`
import $ivy.`org.apache.hudi:hudi-spark-bundle_2.12:0.7.0`
import $ivy.`org.apache.spark:spark-avro_2.12:3.1.1`
import $ivy.`org.apache.spark:spark-sql_2.12:3.1.1`
import $ivy.`org.apache.parquet:parquet-hive-bundle:1.11.1`

import collection.JavaConverters._

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode._

import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._


Logger.getLogger("org").setLevel(Level.OFF)

// Almond version
// val spark = {
//   NotebookSparkSession.builder()
//     .master("local[*]")
//     .appName("MvSparkNotebook")
//     .config("spark.master", "local")
//     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//     .getOrCreate()
// }

val spark = SparkSession
  .builder()
  .appName("MvSparkNotebook")
  .config("spark.master", "local")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .getOrCreate()

val tableName = "hudi_trips_cow"
val basePath = "file:///tmp/hudi_trips_cow"
val dataGen = new DataGenerator

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts.asScala.toSeq, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)

val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath + "/*/*/*/*")
//load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

val updates = convertToStringList(dataGen.generateUpdates(10))
val updatesDf = spark.read.json(spark.sparkContext.parallelize(updates.asScala.toSeq, 2))

updatesDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

spark.
  read.
  format("hudi").
  load(basePath + "/*/*/*/*").
  createOrReplaceTempView("hudi_trips_snapshot")

import spark.implicits._

val commits = spark.sql(
  "select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime"
).map(k => k.getString(0)).take(50)
val beginTime = commits(commits.length - 2) // commit time we are interested in

val tripsIncrementalDF = spark.read.format("hudi").
  option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
  load(basePath)
tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()

val beginTimeAll = "000" // Represents all commits > this time.
val endTime = commits(commits.length - 2) // commit time we are interested in
val tripsPointInTimeDF = spark.read.format("hudi").
  option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
  option(BEGIN_INSTANTTIME_OPT_KEY, beginTimeAll).
  option(END_INSTANTTIME_OPT_KEY, endTime).
  load(basePath)
tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")

spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()

spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
val ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

val deletes = dataGen.generateDeletes(ds.collectAsList())
val deletesDf = spark.read.json(spark.sparkContext.parallelize(deletes.asScala.toSeq, 2))

getQuickstartWriteConfigs

deletesDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY, "delete").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

val roAfterDeleteViewDF = spark.
  read.
  format("hudi").
  load(basePath + "/*/*/*/*")
roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()

spark.
  read.format("hudi").
  load(basePath + "/*/*/*/*").
  select("uuid", "partitionpath").
  show(10, false)


val insertsNew = convertToStringList(dataGen.generateInserts(10))
val insertsDf = spark.read.json(spark.sparkContext.parallelize(insertsNew.asScala.toSeq, 2))

insertsDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY, "insert_overwrite_table").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

spark.
  read.format("hudi").
  load(basePath + "/*/*/*/*").
  select("uuid", "partitionpath").
  show(10, false)

spark.
  read.format("hudi").
  load(basePath + "/*/*/*/*").
  select("uuid", "partitionpath").
  sort("partitionpath", "uuid").
  show(100, false)
val inserts4Partition = convertToStringList(dataGen.generateInserts(10))
val partitionDf = spark.
  read.json(spark.sparkContext.parallelize(inserts4Partition.asScala.toSeq, 2)).
  filter("partitionpath = 'americas/united_states/san_francisco'")

partitionDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY, "insert_overwrite").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

spark.
  read.format("hudi").
  load(basePath + "/*/*/*/*").
  select("uuid", "partitionpath").
  sort("partitionpath", "uuid").
  show(100, false)

import scala.sys.process._

s"tree -L 5 /tmp/hudi_trips_cow".!
