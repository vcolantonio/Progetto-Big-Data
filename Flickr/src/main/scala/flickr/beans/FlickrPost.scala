package flickr.beans

import java.time.Instant

case class FlickrPost(description: String, title: String, urls: List[String], id: String,
                      datePosted: Instant, views: Long, owner: FlickrPostOwner,
                      comments: Long, originalHeight: Long, familyFlag: Boolean,
                      geoData: FlickrGeoData, url: String, tags: List[FlickrPostTag],
                      lastUpdate: Instant, dateTaken: Instant
                     ) {

}
