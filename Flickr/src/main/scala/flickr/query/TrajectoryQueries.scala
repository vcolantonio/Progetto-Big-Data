package flickr.query

import flickr.beans.FlickrPost
import flickr.geo.{GeoDFItem, GeoItem}
import flickr.utils.Utils._
import org.apache.spark.ml.fpm.PrefixSpan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.{Instant, LocalDateTime, ZoneOffset}

class TrajectoryQueries(spark: SparkSession) extends Serializable {

  import spark.implicits._

  def seq_for_prefix_span(sequences: DataFrame): DataFrame = {
    val all_sequences_df =
      sequences
        .map(x => x.get(2).asInstanceOf[Seq[String]].map(y => Seq(y)))
        .toDF("sequence")

    all_sequences_df.write.json("all_sequences_json")
    all_sequences_df
  }

  def load_seq_for_prefix_span(sequences: DataFrame): DataFrame = {
    spark.read.json("all_sequences_json")
  }

  def topPatterns(howMany: Int = 5, filter: String = null, minLength: Int = 1): DataFrame = {
    val freq_patterns = spark.read.json("freq_patterns").repartition(20)

    val freq_patterns_lengths_ds = freq_patterns.map(x =>
      (
        x(0).asInstanceOf[Long],
        x(1).asInstanceOf[Seq[Seq[String]]],
        x(2).asInstanceOf[Double],
        x(1).asInstanceOf[Seq[Seq[String]]].length
      )
    ).filter(x => x._4 >= minLength)

    var freq_patterns_lengths: Dataset[Row] = null

    if (filter != null && !filter.equals(""))
      freq_patterns_lengths = freq_patterns_lengths_ds
        .filter(
          x => x._2
            .flatMap(_.toStream).mkString(" ").toLowerCase.contains(filter.strip.toLowerCase))
        .toDF("count", "sequence", "supp", "length")
        .sort(desc("length"), desc("supp"))
    else
      freq_patterns_lengths = freq_patterns_lengths_ds
        .toDF("count", "sequence", "supp", "length")
        .sort(desc("length"), desc("supp"))

    freq_patterns_lengths
      .map(x =>
        (
          x(3).asInstanceOf[Int].toLong,
          (
            x(1).asInstanceOf[Seq[Seq[String]]],
            x(2).asInstanceOf[Double],
            x(0).asInstanceOf[Long]
          )
        )
      )
      .toDF("length", "tuple")
      .groupBy("length")
      .agg(collect_list("tuple"))
      .map(y =>
        (
          y(0).asInstanceOf[Long],
          y(1).asInstanceOf[Seq[Row]].sortBy(z => -z(1).asInstanceOf[Double])
            .slice(0, howMany) // limita a howMany per ogni gruppo
            .map(
              t =>
                (
                  t(0).asInstanceOf[Seq[Seq[String]]],
                  t(1).asInstanceOf[Double],
                  t(2).asInstanceOf[Long]
                )
            )
        )
      )

      .flatMap(x => x._2)
      .map(x => (x._1.length, x._1, x._2, x._3))
      .toDF("PATTERN_LENGTH", "PATTERN", "SUPPORT", "COUNT")
      .sort(asc("PATTERN_LENGTH"), desc("SUPPORT"), desc("COUNT"))

  }

  def frequent_seq_pat(dataframe: DataFrame): DataFrame = {
    val result = new PrefixSpan()
      .setMinSupport(0.02)
      .setMaxPatternLength(10)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(dataframe)

    val tot = dataframe.count()

    val freq_supp = result.map(x =>
      (x(0).asInstanceOf[Seq[Seq[String]]], x(1).asInstanceOf[Long], x(1).asInstanceOf[Long] / (1.0 * tot)))
      .toDF("sequence", "count", "supp")
    freq_supp.write.json("freq_patterns")

    freq_supp
  }

  private def user_loc_seq(dataset: Dataset[FlickrPost], datasetGeo: Dataset[GeoDFItem]): DataFrame = {
    val res = best_loc_guess_all_data(dataset, datasetGeo).repartition(200)
      .withColumn("DATE", to_date(col("TIMESTAMP"), "yyyy-MM-dd"))

    val res2 = res
      .groupBy("NAME").count()

    val all_sequences = res.repartition(200)
      .join(res2, "NAME")
      .sort(asc("DATE"), asc("TIMESTAMP"), asc("ID_OWNER"), desc("count"))

      .dropDuplicates("DATE", "ID_OWNER", "NAME")
      .groupBy("DATE", "ID_OWNER")
      .agg(collect_list("NAME"))
      .toDF("DATE", "ID_OWNER", "SEQUENCE")

    all_sequences.write.json("sequences_json")

    all_sequences
  }

  private def best_loc_guess_all_data(dataset: Dataset[FlickrPost], datasetGeo: Dataset[GeoDFItem]): DataFrame = {
    preprocess_for_trajectory_mining(dataset).repartition(200)
      .map(x => (x.owner.id, x.id, x.geoData.latitude, x.geoData.longitude, x.dateTaken))
      .crossJoin(datasetGeo.map(x => (x.lat, x.lon, x.address.getByName(x.category) + " | " + x.address.road)).toDF("_7", "_8", "_9"))
      .map(x => (x.get(x.fieldIndex("_1")).asInstanceOf[String], x.get(x.fieldIndex("_2")).asInstanceOf[String],
        x.get(x.fieldIndex("_7")).asInstanceOf[Double], x.get(x.fieldIndex("_8")).asInstanceOf[Double],
        x.get(x.fieldIndex("_9")).asInstanceOf[String],
        dist(
          GeoItem(x.get(x.fieldIndex("_3")).asInstanceOf[Double], x.get(x.fieldIndex("_4")).asInstanceOf[Double]),
          GeoItem(x.get(x.fieldIndex("_7")).asInstanceOf[Double], x.get(x.fieldIndex("_8")).asInstanceOf[Double])),
        x.get(x.fieldIndex("_5")).asInstanceOf[Instant]))
      .toDF("ID_OWNER", "ID_POST", "LAT", "LON", "NAME", "DIST", "TIMESTAMP") // | ID (utente) | ID (post) | LAT | LONG | NAME | CAT | TYPE | TIMESTAMP |
      .filter(x => x.get(x.fieldIndex("DIST")).asInstanceOf[Int] < 300)
      .sort("ID_POST", "DIST")
  }

  private def dist(item1: GeoItem, item2: GeoItem): Int = {
    val latDistance = Math.toRadians(item1.getLatitude - item2.getLatitude)
    val lngDistance = Math.toRadians(item1.getLongitude - item2.getLongitude)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(item1.getLatitude)) *
        Math.cos(Math.toRadians(item2.getLatitude)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_R * c * 1000).toInt
  }

  private def preprocess_for_trajectory_mining(dataset: Dataset[FlickrPost]): Dataset[FlickrPost] = {
    dataset
      .filter(_.dateTaken != null)
      .filter(_.dateTaken.isAfter(LocalDateTime.of(2004, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)))
      .filter(_.dateTaken.isBefore(LocalDateTime.of(2020, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)))
      .filter(x => x.geoData != null && x.geoData.latitude != null && x.geoData.longitude != null)
  }

  private def load_user_loc_seq(): DataFrame = {
    spark.read.json("sequences_json")
  }

  private def best_loc_guess_dataset(dataset: Dataset[(FlickrPost, (String, GeoDFItem, Int))]): Array[(FlickrPost, (String, GeoDFItem, Int))] = {
    val a = dataset.collect()

    return a
  }

  private def getPartitionByDate(date: String, dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(x => x.get(x.fieldIndex("DATE")).toString.equals(date))
      .dropDuplicates("NAME", "ID_OWNER")
      .sort("ID_OWNER", "TIMESTAMP")
  }

  private def getPartitionByUser(userId: String, dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(x => x.get(x.fieldIndex("ID_OWNER")).toString.equals(userId))
      .sort("TIMESTAMP")
  }

}


