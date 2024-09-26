/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;

/**
 * Geohash scalar functions that can be used in transformation.
 * This class is used to encode and decode geohash values.
 */
public class GeohashFunctions {
  private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";
  private static final int[] BITS = {16, 8, 4, 2, 1};

  private GeohashFunctions() {
  }

  public static long encode(double latitude, double longitude, int length) {
    if (length < 1 || length > 12) {
      throw new IllegalArgumentException("length must be between 1 and 12");
    }

    boolean isEven = true;
    double minLat = -90.0;
    double maxLat = 90.0;
    double minLon = -180.0;
    double maxLon = 180.0;
    long bit = 0x8000000000000000L;
    long geohash = 0L;

    for (long i = 0; i < 5 * length; i++) {
      if (isEven) {
        double mid = (minLon + maxLon) / 2;
        if (longitude >= mid) {
          geohash |= bit;
          minLon = mid;
        } else {
          maxLon = mid;
        }
      } else {
        double mid = (minLat + maxLat) / 2;
        if (latitude >= mid) {
          geohash |= bit;
          minLat = mid;
        } else {
          maxLat = mid;
        }
      }

      isEven = !isEven;
      bit >>>= 1;
    }

    return geohash | length;
  }

  private static String longHashToStringGeohash(long hash) {
    int length = (int) (hash & 15L);
    if (length < 1 || length > 12) {
      throw new IllegalArgumentException("Invalid geohash length: " + length);
    }

    char[] geohash = new char[length];
    for (int i = 0; i < length; i++) {
      geohash[i] = BASE32.charAt((int) ((hash >>> 59) & 31L));
      hash <<= 5;
    }

    return new String(geohash);
  }

  public static double[] decode(String geohash) {
    double[] lat = {-90.0, 90.0};
    double[] lon = {-180.0, 180.0};
    boolean isEven = true;

    for (int i = 0; i < geohash.length(); i++) {
      int cd = BASE32.indexOf(geohash.charAt(i));
      for (int j = 0; j < 5; j++) {
        int mask = BITS[j];
        if (isEven) {
          refineInterval(lon, cd, mask);
        } else {
          refineInterval(lat, cd, mask);
        }
        isEven = !isEven;
      }
    }

    return new double[]{(lat[0] + lat[1]) / 2, (lon[0] + lon[1]) / 2};
  }

  private static void refineInterval(double[] interval, int cd, int mask) {
    if ((cd & mask) != 0) {
      interval[0] = (interval[0] + interval[1]) / 2;
    } else {
      interval[1] = (interval[0] + interval[1]) / 2;
    }
  }

  /**
   * Encodes a latitude and longitude to a geohash.
   * @param latitude
   * @param longitude
   * @param precision
   * @return the geohash value as a string
   */
  @ScalarFunction
  public static String encodeGeoHash(double latitude, double longitude, int precision) {
    return longHashToStringGeohash(encode(latitude, longitude, precision));
  }

  /**
   * Decodes a geohash to a latitude and longitude.
   * @param geohash
   * @return the latitude and longitude as a double array
   */
  @ScalarFunction
  public static double[] decodeGeoHash(String geohash) {
    return decode(geohash);
  }

  /**
   * Decodes a geohash to a latitude.
   * @param geohash
   * @return the latitude as a double
   */
  @ScalarFunction(names = {"decodeGeoHashLatitude", "decodeGeoHashLat"})
  public static double decodeGeoHashLatitude(String geohash) {
    double[] latLon = decode(geohash);
    return latLon[0];
  }

  /**
   * Decodes a geohash to a longitude.
   * @param geohash
   * @return the longitude as a double
   */
  @ScalarFunction(names = {"decodeGeoHashLongitude", "decodeGeoHashLon"})
  public static double decodeGeoHashLongitude(String geohash) {
    double[] latLon = decode(geohash);
    return latLon[1];
  }
}
