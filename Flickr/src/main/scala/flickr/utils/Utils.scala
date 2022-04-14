package flickr.utils

import org.apache.spark.sql.functions.udf

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Locale

object Utils {

  val LIMIT = 1000000000
  val MIN_TAG_COUNT = 100
  val AVERAGE_R = 6371

  @transient val formatter = DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a")
    .withLocale(Locale.ENGLISH)
  @transient val to_date_en = udf((x: String) => LocalDateTime.parse(x, formatter)
    .toInstant(ZoneOffset.UTC))
  @transient val to_date_en_year =
    udf((x: String) => x.split("-")(0))
  @transient val to_date_en_month =
    udf((x: String) => x.split("-")(0) + "-" + x.split("-")(1)) //anno-mese

}
