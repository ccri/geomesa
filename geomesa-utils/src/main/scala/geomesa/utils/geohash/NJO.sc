import geomesa.utils.geohash.GeoHash
import geomesa.utils.geohash.NextJumpBS._


val ll = GeoHash(1.0, 1.0, 15)
val ur =  GeoHash(20.0, 10.0, 15)
val vs =  violations(ll, ur, ll)
 njo(ll, ur, ll, vs)
