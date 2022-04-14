package flickr.spark

import flickr.beans.FlickrPost
import flickr.geo.GeoDFItem
import flickr.utils.Utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Locale

object SparkInitializer {

  val LIMIT = 1000000000
  val MIN_TAG_COUNT = 100

  val AVERAGE_R = 6371

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("Flickr Project")
    .config("spark.master", "local[8]")
    .config("spark.locality.wait", "0s")
    .config("dfs.client.read.shortcircuit.skip.checksum", "true")
    .config(SQLConf.DATETIME_JAVA8API_ENABLED.key, "true")
    .getOrCreate()


  import spark.implicits._

  implicit val e = org.apache.spark.sql.Encoders.DATE

  val dataset = load_flickr_dataset("FlickrRome2017.json")
  val datasetGeo = load_geodata_dataset("geoDF.json")

  def main(args: Array[String]): Unit = {


  }

  def load_flickr_dataset(fileName: String): Dataset[FlickrPost] = {
    var dataset_df = spark.read.json(fileName).limit(LIMIT)

    dataset_df = dataset_df.withColumn("dateTaken", to_date_en(col("dateTaken")))
    dataset_df = dataset_df.withColumn("datePosted", to_date_en(col("datePosted")))
    dataset_df = dataset_df.withColumn("lastUpdate", to_date_en(col("lastUpdate")))

    dataset_df.drop("server", "notes", "publicFlag", "placeId", "originalFormat",
      "secret", "media", "iconServer", "farm", "hasPeople", "originalSecret", "originalHeight",
      "familyFlag", "rotation", "mediaStatus", "friendFlag", "originalWidth", "license",
      "iconFarm", "favorite", "primary", "pathAlias")

    val dataset = dataset_df.as[FlickrPost]

    dataset.repartition(200)
  }

  def load_geodata_dataset(fileName: String): Dataset[GeoDFItem] = {
    var datasetGeo: Dataset[GeoDFItem] = spark.read.json("geoDF.json").withColumnRenamed("type", "typology")
      .na.drop(Seq("lat")).na.drop(Seq("lon"))
      .withColumn("lat", 'lat.cast(DoubleType))
      .withColumn("lon", 'lon.cast(DoubleType))
      .as[GeoDFItem]

    datasetGeo = datasetGeo.drop("licence").as[GeoDFItem]
    datasetGeo = datasetGeo.drop("icon").as[GeoDFItem]
    datasetGeo = datasetGeo.drop("osm_type").as[GeoDFItem]

    val important_cat_types = spark.read.option("header", "true").csv("cat_type.csv").as[(String, String)]
    val list_imp = important_cat_types.collect()
    datasetGeo = datasetGeo.filter(x => list_imp.count(a => a._1 == x.category && a._2 == x.typology) == 1)

    datasetGeo
  }

}
