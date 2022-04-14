package flickr.query

import flickr.beans.{FlickrPost, FlickrPostTag}
import flickr.utils.Utils._
import org.apache.spark.sql.functions.{col, desc, to_date}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TagQueries(spark: SparkSession) extends Serializable {

  import spark.implicits._

  /*
  dato un tag, mostrare i post associati ad esso
   */
  def postForTag(dataset: Dataset[FlickrPost], tag: String): Dataset[Row] = {
    val a = dataset.filter(_.tags.contains(new FlickrPostTag(0, tag)))
      .map(x => (x.id, x.url, x.tags)).toDF("idPost", "url", "tags")

    a
  }

  def trendTagByYear(dataset: Dataset[FlickrPost], tag: String): Dataset[Row] = {
    val a = trendTagByDay(dataset, tag).withColumn("year",
      to_date_en_year(col("date"))).drop("date")
    val b = a.groupBy("year").agg(Map("number of post" -> "sum"))
      .withColumnRenamed("sum(number of post)", "total post")

    b
  }

  /*
  trend di un tag nel tempo --> numero di post associati al tag
   */
  def trendTagByDay(dataset: Dataset[FlickrPost], tag: String): Dataset[Row] = {
    val post = dataset.filter(_.tags.contains(new FlickrPostTag(0, tag)))
      .map(x => (x.id, x.datePosted, x.tags)).toDF("idPost", "datePosted", "tags")
      .withColumn("date", to_date(col("datePosted"), "yyyy-MM-dd"))
      .drop("datePosted")
    //ora raggruppo per date --> numero di post con quel tag in quella data
    val group = post.groupBy("date").count().toDF("date", "number of post")
    post.join(group, "date").dropDuplicates("date").drop("idPost")
  }

  def trendTagByMonth(dataset: Dataset[FlickrPost], tag: String): Dataset[Row] = {
    val a = trendTagByDay(dataset, tag).withColumn("month",
      to_date_en_month(col("date")))
      .drop("date")
    val b = a.groupBy("month").agg(Map("number of post" -> "sum"))
      .withColumnRenamed("sum(number of post)", "total post")

    b
  }

  def mostUsedTags(dataset: Dataset[FlickrPost]): Dataset[Row] = {
    val tags = dataset
      .map(x => x.tags.map(y => (y.value, x.views, 1)))
      .flatMap(x => x)
      .groupByKey(x => x._1)
      .mapValues(x => (x._2, x._3))
      .reduceGroups((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1, x._2._2))
      .toDF("tag", "views", "count")
      .sort(desc("views"), desc("count"))

    tags

  }

}