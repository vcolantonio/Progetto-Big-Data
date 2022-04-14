package flickr.query

import com.johnsnowlabs.nlp.annotator.{Normalizer, StopWordsCleaner, Tokenizer, UniversalSentenceEncoder}
import com.johnsnowlabs.nlp.{DocumentAssembler, EmbeddingsFinisher}
import flickr.beans.{FlickrPost, UserEmbedding}
import flickr.spark.SparkInitializer._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.functions.{asc, collect_list}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.{LocalDateTime, ZoneOffset}

class UserClusteringQueries(spark: SparkSession) extends Serializable {

  import spark.implicits._

  def userEmbeddings(dataset: Dataset[FlickrPost], normalize: Boolean = true): DataFrame = {

    val document = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val embedder = UniversalSentenceEncoder.pretrained("tfhub_use_multi", "xx")
      .setInputCols("document")
      .setOutputCol("sentence_embeddings")

    val embeddingsFinisher = new EmbeddingsFinisher()
      .setInputCols("sentence_embeddings")
      .setOutputCols("finished_embeddings")
      .setOutputAsVector(true)

    val pipeline = new Pipeline().setStages(Array(
      document,
      embedder,
      embeddingsFinisher)
    )

    val documentDF = dataset.filter(x => (x.title != null && x.description != null)).repartition(200)
      .map(x => (x.owner.id, get_text(x.title, x.description)))
      .toDF("user", "text")

    val embeddings = pipeline.fit(documentDF).transform(documentDF)
      .selectExpr("user", "text", "explode(finished_embeddings) as embedding")

    val user_embeddings =
      embeddings.groupByKey(x => x(0).toString).mapValues(x => (x(2).asInstanceOf[DenseVector].toArray.toSeq, 1))
        .reduceGroups((a, b) => (a._1.zip(b._1).map(c => c._1 + c._2), a._2 + b._2))

    user_embeddings.write.json("post_plus_embedding")

    if (!normalize)
      user_embeddings.map(x => (x._1, x._2._1))
        .toDF("user", "embedding")

    else
      user_embeddings.map(x => (x._1, x._2._1.map(z => z / (1.0 * x._2._2))))
        .toDF("user", "embedding")

  }

  private def get_text(title: String, description: String): String = {
    if (title == null && description == null)
      ""
    else if (title == null)
      description
    else if (description == null)
      title
    else
      title + " " + description
  }

  def statistics_for_cluster(i: Int, k: Int, normalize: Boolean): (Long, Double, Double, Double) = {
    val user_for_cluster = load_user_cluster_assignments(k, normalize).filter(x => x(1).asInstanceOf[Int] == i)

    val ds1 = preprocess_dataset_for_clustering(dataset)
      .filter(x => x.datePosted.isBefore(LocalDateTime.of(2004, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)) ||
        x.datePosted.isAfter(LocalDateTime.of(2020, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)))
      .map(x => (x.owner.id, x.title, x.description, x.tags, x.views, x.datePosted.toEpochMilli))
      .toDF("user", "title", "description", "tags", "views", "datePosted")

    val ds2 = preprocess_dataset_for_clustering(dataset)
      .filter(_.datePosted.isAfter(LocalDateTime.of(2004, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)))
      .filter(_.datePosted.isBefore(LocalDateTime.of(2020, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)))
      .map(x => (x.owner.id, x.title, x.description, x.tags, x.views, x.datePosted.toEpochMilli))
      .toDF("user", "title", "description", "tags", "views", "datePosted")

    val posts_in_cluster1 = user_for_cluster
      .join(ds1, "user")

    val posts_in_cluster2 = user_for_cluster
      .join(ds2, "user")

    val numUsers = user_for_cluster.count()

    val numPostsFiltered1 = posts_in_cluster1.count()
    val numPostsFiltered2 = posts_in_cluster2.count()

    var res1 = (0l, 0l, 0d)
    var res2 = (0l, 0l, 0d)

    if (numPostsFiltered1 != 0) {

      val postCount_viewsCount1 = posts_in_cluster1
        .groupByKey(x => x(x.fieldIndex("user")).toString)
        .mapValues(a => (1L, a(a.fieldIndex("views")).asInstanceOf[Long]))
        .reduceGroups((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(x => (x._1, x._2._1, x._2._2))
        .toDF("user", "tot_posts", "tot_views")


      val tmp = postCount_viewsCount1
        .map(x => (
          x(x.fieldIndex("tot_posts")).asInstanceOf[Long],
          x(x.fieldIndex("tot_views")).asInstanceOf[Long],
        ))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

      res1 = (tmp._1, tmp._2, -1)

    }

    if (numPostsFiltered2 != 0) {
      val min_date = posts_in_cluster2
        .groupBy("user")
        .min("datePosted")
        .toDF("user", "minDatePosted")

      val max_date = posts_in_cluster2
        .groupBy("user")
        .max("datePosted")
        .toDF("user", "maxDatePosted")

      val daysUserActive = min_date
        .join(max_date, "user")
        .map(x =>
          (
            x(x.fieldIndex("user")).toString,
            (x(x.fieldIndex("maxDatePosted")).asInstanceOf[Long] -
              x(x.fieldIndex("minDatePosted")).asInstanceOf[Long])
              / (1.0 * 1000 * 60),
            x(x.fieldIndex("minDatePosted")).asInstanceOf[Long],
            x(x.fieldIndex("maxDatePosted")).asInstanceOf[Long]
          )
        )
        .toDF("user", "minutes", "min", "max")

      val postCount_viewsCount2 = posts_in_cluster2
        .groupByKey(x => x(x.fieldIndex("user")).toString)
        .mapValues(a => (1L, a(a.fieldIndex("views")).asInstanceOf[Long]))
        .reduceGroups((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(x => (x._1, x._2._1, x._2._2))
        .toDF("user", "tot_posts", "tot_views")

      val postFrequency =
        daysUserActive
          .join(postCount_viewsCount2, "user")
          .map(x => (
            x(x.fieldIndex("user")).toString, {
            if (x(x.fieldIndex("minutes")).asInstanceOf[Double] != 0) {
              x(x.fieldIndex("tot_posts")).asInstanceOf[Long] / (1.0 * x(x.fieldIndex("minutes")).asInstanceOf[Double])
            } else 0
          }
          ))
          .toDF("user", "frequency")


      res2 = postCount_viewsCount2
        .join(postFrequency, "user")
        .map(x => (
          x(x.fieldIndex("tot_posts")).asInstanceOf[Long],
          x(x.fieldIndex("tot_views")).asInstanceOf[Long],
          x(x.fieldIndex("frequency")).asInstanceOf[Double]
        ))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    }

    var a = 0d
    var b = 0d
    var c = 0d

    a = (res1._1 + res2._1) / numUsers
    b = (res1._2 + res2._2) / numUsers
    c = if (numPostsFiltered2 != 0) res2._3 / numUsers else -1

    (numUsers, a, b, c)

  }

  def load_user_cluster_assignments(k: Int, normalize: Boolean): DataFrame = {
    val embeddings = load_user_embeddings(normalize)
      .map(x => (x.user, Vectors.dense(x.embedding))).toDF("user", "embedding")

    val kmeansModel = userClustering(k, embeddings)
    val clusterAssignments = kmeansModel.transform(embeddings)
      .selectExpr("user", "prediction", "embedding").toDF("user", "cluster", "embedding")

    clusterAssignments
  }

  def userClustering(k: Int, embeddings: DataFrame): KMeansModel = {
    val kmeans = new KMeans()
      .setK(k)
      .setSeed(5L)
      .setMaxIter(100)
      .setInitSteps(2)
      .setInitMode("k-means||")
      .setFeaturesCol("embedding")
      .setPredictionCol("prediction")
    val kmeansModel = kmeans.fit(embeddings)

    kmeansModel
  }

  def load_user_embeddings(normalize: Boolean): Dataset[UserEmbedding] = {
    val user_embeddings = spark.read.json("post_plus_embedding")
      .as[(Tuple2[Seq[Double], Long], String)]

    if (!normalize)
      user_embeddings.map(x => (x._2, x._1._1))
        .toDF("user", "embedding").as[UserEmbedding]

    else
      user_embeddings.map(x => (x._2, x._1._1.map(z => z / (1.0 * x._1._2))))
        .toDF("user", "embedding").as[UserEmbedding]

  }

  def preprocess_dataset_for_clustering(dataset: Dataset[FlickrPost]) = {
    dataset
      .filter(x => x.title != null && x.description != null)
  }

  def representatives(howMany: Int = 5, normalize: Boolean, k: Int): DataFrame = {
    val embeddings = load_user_embeddings(normalize)
      .map(x => (x.user, Vectors.dense(x.embedding))).toDF("user", "embedding")

    val kmeansModel = userClustering(k, embeddings)
    val user_emb_cl = kmeansModel.transform(embeddings)
      .selectExpr("user", "prediction", "embedding").toDF("user", "cluster", "embedding")

    val centroids = kmeansModel.clusterCenters.zipWithIndex

    val user_emb_cl_dist = user_emb_cl.map(x =>
      (
        x(0).asInstanceOf[String],
        x(1).asInstanceOf[Int],
        Vectors.sqdist(
          centroids.find(a => a._2 == x(1).asInstanceOf[Int]).get._1,
          Vectors.dense(x(2).asInstanceOf[DenseVector].toArray)
        )
      )
    ).toDF("user", "cluster", "dist")

    user_emb_cl_dist
      .map(x =>
        (
          x(1).asInstanceOf[Int],
          (
            x(0).asInstanceOf[String],
            x(2).asInstanceOf[Double]
          )
        )
      )
      .toDF("cluster", "tuple")
      .groupBy("cluster")
      .agg(collect_list("tuple"))
      .map(y =>
        (
          y(0).asInstanceOf[Int],
          y(1).asInstanceOf[Seq[Row]].sortBy(z => z(1).asInstanceOf[Double])
            .slice(0, howMany)
            .map(
              t =>
                (
                  t(0).asInstanceOf[String],
                  t(1).asInstanceOf[Double],
                  y(0).asInstanceOf[Int],
                )
            )
        )
      )

      .flatMap(x => x._2)
      .map(x => (x._3, x._1, x._2))
      .toDF("CLUSTER", "USER", "DISTANCE")
      .sort(asc("CLUSTER"), asc("DISTANCE"))

  }

}


