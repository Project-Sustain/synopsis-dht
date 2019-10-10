/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package sustain.synopsis.ingestion.client.geohash;

/**
 * Encapsulates a point in space with latitude, longitude coordinates.
 *
 * @author malensek
 */
public class Coordinates {
    private float lat;
    private float lon;

    /**
     * Create Coordinates at the specified latitude and longitude.
     *
     * @param lat
     *     Latitude for this coordinate pair, in degrees.
     * @param lon
     *     Longitude for this coordinate pair, in degrees.
     */
    public Coordinates(float lat, float lon) {
        this.lat = lat;
        this.lon = lon;
    }

    /**
     * Get the latitude of this coordinate pair.
     *
     * @return latitude, in degrees.
     */
    public float getLatitude() {
        return lat;
    }

    /**
     * Get the longitude of this coordinate pair.
     *
     * @return longitude, in degrees
     */
    public float getLongitude() {
        return lon;
    }

    /**
     * Print this coordinate pair's String representation:
     * (lat, lon).
     *
     * @return String representation of the Coordinates
     */
    @Override
    public String toString() {
        return "(" + lat + ", " + lon + ")";
    }
}
