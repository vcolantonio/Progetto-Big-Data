package flickr.beans

case class FlickrPostTag(count: Long, value: String) {

  override def toString: String = "#" + value

  override def equals(obj: Any): Boolean = {
    if (obj == null) false

    if (!obj.isInstanceOf[FlickrPostTag]) false

    val a = obj.asInstanceOf[FlickrPostTag]
    a.value.equals(value)

  }

}
