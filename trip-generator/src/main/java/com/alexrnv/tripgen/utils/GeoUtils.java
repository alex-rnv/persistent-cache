package com.alexrnv.tripgen.utils;

/**
 * Created by Asolovyev on 24/04/2015.
 */
public class GeoUtils {
    public static final double metersInDeg = 111190.0;
    public static final double earthRadius = 6371000.0;

    /** Calculate distance between two lat-lon points using spherical approximation of Earth
     * Note - it is not the most accurate model of earth, but rather fast calculations.
     * However the error is far below 1%.
     * http://en.wikipedia.org/wiki/Haversine_formula
     * @return distance in metres between two points
     */
    public static double distanceM(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2-lat1);
        double dLon = Math.toRadians(lon2-lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon/2) * Math.sin(dLon/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double dist = (earthRadius * c);

        return dist;
    }
}
