package flickr.query

import com.microsoft.azure.synapse.ml.onnx.ONNXModel
import com.microsoft.azure.synapse.ml.opencv.ImageTransformer
import flickr.beans.FlickrPost
import org.apache.spark.sql.{Dataset, SparkSession}


class MLQueries(spark: SparkSession) extends Serializable {

  import spark.implicits._

  def imageNet(filename: String): Array[Seq[(Float, String)]] = {
    val onnx = new ONNXModel().setModelLocation("googlenet-12.onnx")

    val inputImg = spark.read.format("image")
      .load("images/" + filename)

    println("Model inputs:" + onnx.modelInput)
    println("Model outputs:" + onnx.modelOutput)

    val image_df = (new ImageTransformer()
      .setInputCol("image")
      .setOutputCol("transformed_images")
      .resize(224, true)
      .centerCrop(224, 224)
      .setNormalizeMean(Array(-123.68, -116.779, -103.939))
      .setToTensor(true)

      .transform(inputImg))

    val m = onnx.setDeviceType("CPU")
      .setFeedDict("data_0", "transformed_images")
      .setFetchDict("probability", "prob_1")
      .setMiniBatchSize(1)

    val probs = m.transform(image_df).coalesce(1).select("probability")
      .map(x => x(0).asInstanceOf[Seq[Float]]).collect()

    val imageNetClasses = spark.read.option("header", "true").csv("imagenet.csv")
      .select("class")
      .map(x => x(0).toString)
      .collect()

    probs.map(x => x.zip(imageNetClasses).sortBy(_._1).reverse)

  }

  def rowLanguage(dataset: Dataset[FlickrPost]) = {
    import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

    val pipeline = new PretrainedPipeline("detect_language_21", lang = "xx")

    dataset.map(x => pipeline.annotate(x.title + " " + x.description))
  } // distribuzione lingua utenti


}


