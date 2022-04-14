package flickr.query

import flickr.beans.FlickrPost
import flickr.geo.{GeoDFItem, GeoItem, OpenStreetMapWrapper}
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.io.File

class OSMQueries(spark: SparkSession) extends Serializable {

  import spark.implicits._

  private def getInfoFromOSM(dataset: Dataset[FlickrPost]): DataFrame = {

    val osmw = OpenStreetMapWrapper

    File("geoDF.json").writeAll("")

    val geoDFEl = dataset
      .filter(x => x.geoData != null && x.geoData.longitude != null && x.geoData.latitude != null)
      .map(x => (x.geoData.latitude.asInstanceOf[Float], x.geoData.longitude.asInstanceOf[Float]))
      .toDF("LAT", "LONG")
      .withColumn("LAT", round(col("LAT"), 3))
      .withColumn("LONG", round(col("LONG"), 3))
      .dropDuplicates("LAT", "LONG")
      .as[(Float, Float)]

    geoDFEl.show(5)
    println(geoDFEl.count())

    geoDFEl
      .map(x => osmw.getInfo(new GeoItem(x._1, x._2)))
      .foreach(x => File("geoDF.json").appendAll(x + "\n"))

    val geoDF = spark.read.json("geoDF.json").toDF()

    return geoDF
  }

  private def countCategory(dataset: Dataset[GeoDFItem]): DataFrame = {

    dataset
      .groupBy(col("category"))
      .count()

  }

  private def countType(dataset: Dataset[GeoDFItem]): DataFrame = {

    dataset
      .groupBy(col("type"))
      .count()

  }

  private def countCatType(dataset: Dataset[GeoDFItem], k: Int) = {

    dataset
      .filter(x => !x.category.equals("highway") && !x.category.equals("place")
        && !x.category.equals("leisure") && !x.category.equals("tourism")
        && !x.category.equals("shop") && !x.category.equals("office")
        && !x.category.equals("natural") && !x.category.equals("military")
        && !x.category.equals("railway"))
      .groupBy("category", "type")
      .count()
      .toDF("category", "type", "count")
      .filter(x => x.get(x.fieldIndex("count")).asInstanceOf[Long] >= k)
      .sort("category")


  }


}


