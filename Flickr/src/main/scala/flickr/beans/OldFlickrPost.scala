package flickr.beans

import java.time.Instant

case class OldFlickrPost(server: String, notes: List[String], publicFlag: Boolean, placeId: String, description: String, secret: String,
                         originalFormat: String, media: String, title: String, iconServer: String, urls: List[String],
                         farm: String, id: String, hasPeople: Boolean, datePosted: Instant, views: Long,
                         originalSecret: String, owner: FlickrPostOwner, comments: Long, originalHeight: Long, familyFlag: Boolean,
                         rotation: Long, mediaStatus: String, geoData: FlickrGeoData, friendFlag: Boolean, url: String,
                         originalWidth: Long, tags: List[FlickrPostTag], license: String, iconFarm: String, lastUpdate: Instant,
                         favorite: Boolean, dateTaken: Instant, primary: Boolean, pathAlias: String
                        ) {

}
