package flickr.beans

case class FlickrPostOwner(photosCount: Long, admin: Boolean, revFamily: Boolean,
                           pro: Boolean, iconServer: Long, iconFarm: Long, revContact: Boolean,
                           filesizeMax: Long, bandwidthUsed: Long, bandwidthMax: Long, id: String,
                           revFriend: Boolean, username: String
                          ) {
}
