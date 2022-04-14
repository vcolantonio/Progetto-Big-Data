package flickr.geo

case class GeoDFItemAddress(aeroway: String, amenity: String, building: String, city: String,
                            club: String, commercial: String, country: String, country_code: String,
                            county: String, craft: String, hamlet: String, highway: String,
                            historic: String, house_number: String, industrial: String, leisure: String,
                            locality: String, man_made: String, military: String,
                            municipality: String, natural: String, neighbourhood: String, office: String,
                            place: String, postcode: String, quarter: String, railway: String,
                            residential: String, retail: String, road: String, shop: String, state: String,
                            suburb: String, tourism: String, town: String, village: String) {


  def getByName(name: String): String = {
    try {
      return this.getClass.getDeclaredField(name).get(this).toString
    }
    catch {
      case e: Exception => return ""
    }
  }


}
