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
package org.apache.pinot.core.geospatial.transform.function;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;


/**
 * Function that measures the distance between the two geo-spatial objects. For geometry type, returns the 2-dimensional
 * cartesian minimum distance (based on spatial ref) between two geometries in projected units. For geography, returns
 * the great-circle distance in meters between two SphericalGeography points. Note that g1, g2 shall have the same type.
 */
public class StDistanceFunction extends BaseBinaryGeoTransformFunction {
  private static final float MIN_LATITUDE = -90;
  private static final float MAX_LATITUDE = 90;
  private static final float MIN_LONGITUDE = -180;
  private static final float MAX_LONGITUDE = 180;
  public static final String FUNCTION_NAME = "ST_Distance";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    return transformGeometryToDoubleValuesSV(valueBlock);
  }

  @Override
  public double transformGeometryToDouble(Geometry firstGeometry, Geometry secondGeometry) {
    if (GeometryUtils.isGeography(firstGeometry) != GeometryUtils.isGeography(secondGeometry)) {
      throw new RuntimeException("The first and second arguments shall either all be geometry or all geography");
    }
    if (GeometryUtils.isGeography(firstGeometry)) {
      return sphericalDistance(firstGeometry, secondGeometry);
    } else {
      return firstGeometry.isEmpty() || secondGeometry.isEmpty() ? Double.NaN : firstGeometry.distance(secondGeometry);
    }
  }

  private static void checkLatitude(double latitude) {
    Preconditions
        .checkArgument(latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE, "Latitude must be between -90 and 90");
  }

  private static void checkLongitude(double longitude) {
    Preconditions.checkArgument(longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE,
        "Longitude must be between -180 and 180");
  }

  private static double sphericalDistance(Geometry leftGeometry, Geometry rightGeometry) {
    Preconditions.checkArgument(leftGeometry instanceof Point, "The left argument must be a point");
    Preconditions.checkArgument(rightGeometry instanceof Point, "The right argument must be a point");
    Point leftPoint = (Point) leftGeometry;
    Point rightPoint = (Point) rightGeometry;

    return greatCircleDistance(leftPoint.getX(), leftPoint.getY(), rightPoint.getX(), rightPoint.getY());
  }

  /**
   * Calculate the distance between two points on Earth.
   * <p>
   * This assumes a spherical Earth, and uses the Vincenty formula. (https://en.wikipedia
   * .org/wiki/Great-circle_distance)
   */
  private static double greatCircleDistance(double longitude1, double latitude1, double longitude2, double latitude2) {
    checkLatitude(latitude1);
    checkLongitude(longitude1);
    checkLatitude(latitude2);
    checkLongitude(longitude2);

    double radianLatitude1 = Math.toRadians(latitude1);
    double radianLatitude2 = Math.toRadians(latitude2);

    double sin1 = Math.sin(radianLatitude1);
    double cos1 = Math.cos(radianLatitude1);
    double sin2 = Math.sin(radianLatitude2);
    double cos2 = Math.cos(radianLatitude2);

    double deltaLongitude = Math.toRadians(longitude1) - Math.toRadians(longitude2);
    double cosDeltaLongitude = Math.cos(deltaLongitude);

    double t1 = cos2 * Math.sin(deltaLongitude);
    double t2 = cos1 * sin2 - sin1 * cos2 * cosDeltaLongitude;
    double t3 = sin1 * sin2 + cos1 * cos2 * cosDeltaLongitude;
    return Math.atan2(Math.sqrt(t1 * t1 + t2 * t2), t3) * GeometryUtils.EARTH_RADIUS_M;
  }
}
