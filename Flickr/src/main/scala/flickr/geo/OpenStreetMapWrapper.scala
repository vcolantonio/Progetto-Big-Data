package flickr.geo

import scalaj.http.Http

object OpenStreetMapWrapper {

  // implicit val jsonDecoder: Decoder[GeoDFItem] = deriveDecoder[GeoDFItem]
  // implicit val nestedDecoder: Decoder[GeoDFAddress] = deriveDecoder[GeoDFAddress]
  // implicit private val derivationConfig: Configuration = Configuration.default.withDefaults


  var i: Long = 0

  def getInfo(geoItem: GeoItem): String = {

    val API_URL: String = "https://nominatim.openstreetmap.org"

    val request = Http(API_URL + "/search")
      .header("Charset", "UTF-8")
      .header("User-Agent", "Mozilla/5.0")
      .param("format", "jsonv2")
      .param("addressdetails", "1")

    val result =
      request
        .param("q", geoItem.getLatitude + ", " + geoItem.getLongitude)
        .execute()
        .body.toString

    println("Processed " + i)
    i += 1
    println(result)

    Thread.sleep(1000)

    return result.substring(1, result.length - 1)
  }

}
