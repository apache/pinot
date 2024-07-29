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
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.PI;
import static java.lang.Math.toRadians;


/**
 * Function that calculates the area of the given geo-spatial object. For geometry type, it returns the 2D Euclidean
 * area of a geometry. For geography, returns the area of a polygon or multi-polygon in square meters using a spherical
 * model for Earth.
 */
public class StAreaFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  public static final String FUNCTION_NAME = "ST_Area";
  private double[] _results;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions
        .checkArgument(arguments.size() == 1, "Exactly 1 argument is required for transform function: %s", getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Argument must be single-valued for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.BYTES
        || transformFunction instanceof LiteralTransformFunction,
        "The first argument must be of type BYTES, but was %s",
        transformFunction.getResultMetadata().getDataType());
    _transformFunction = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_results == null) {
      _results = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    byte[][] values = _transformFunction.transformToBytesValuesSV(valueBlock);
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numDocs; i++) {
      Geometry geometry = GeometrySerializer.deserialize(values[i]);
      _results[i] = GeometryUtils.isGeography(geometry) ? calculateGeographyArea(geometry) : geometry.getArea();
    }
    return _results;
  }

  private double calculateGeographyArea(Geometry geometry) {
    Polygon polygon = (Polygon) geometry;

    // See https://www.movable-type.co.uk/scripts/latlong.html
    // and http://osgeo-org.1560.x6.nabble.com/Area-of-a-spherical-polygon-td3841625.html
    // and https://www.element84.com/blog/determining-if-a-spherical-polygon-contains-a-pole
    // for the underlying Maths

    double sphericalExcess = Math.abs(computeSphericalExcess(polygon.getExteriorRing()));

    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      sphericalExcess -= Math.abs(computeSphericalExcess(polygon.getInteriorRingN(i)));
    }

    // Math.abs is required here because for Polygons with a 2D area of 0
    return Math.abs(sphericalExcess * GeometryUtils.EARTH_RADIUS_M * GeometryUtils.EARTH_RADIUS_M);
  }

  private static double computeSphericalExcess(LineString lineString) {
    if (lineString.getNumPoints() < 3) {
      // A path with less than 3 distinct points is not valid for calculating an area
      throw new RuntimeException("Polygon is not valid: a loop contains less then 3 vertices.");
    }

    // Initialize the calculator with the last point
    SphericalExcessCalculator calculator = new SphericalExcessCalculator(lineString.getEndPoint());

    // Our calculations rely on not processing the same point twice
    for (int i = 1; i < lineString.getNumPoints(); i++) {
      calculator.add(lineString.getPointN(i));
    }
    return calculator.computeSphericalExcess();
  }

  private static class SphericalExcessCalculator {
    private static final double TWO_PI = 2 * Math.PI;
    private static final double THREE_PI = 3 * Math.PI;

    private double _sphericalExcess;
    private double _courseDelta;

    private boolean _firstPoint;
    private double _firstInitialBearing;
    private double _previousFinalBearing;

    private double _previousPhi;
    private double _previousCos;
    private double _previousSin;
    private double _previousTan;
    private double _previousLongitude;

    private boolean _done;

    public SphericalExcessCalculator(Point endPoint) {
      _previousPhi = toRadians(endPoint.getY());
      _previousSin = Math.sin(_previousPhi);
      _previousCos = Math.cos(_previousPhi);
      _previousTan = Math.tan(_previousPhi / 2);
      _previousLongitude = toRadians(endPoint.getX());
      _firstPoint = true;
    }

    private void add(Point point)
        throws IllegalStateException {
      checkState(!_done, "Computation of spherical excess is complete");

      double phi = toRadians(point.getY());
      double tan = Math.tan(phi / 2);
      double longitude = toRadians(point.getX());

      // We need to check for that specifically
      // Otherwise calculating the bearing is not deterministic
      if (longitude == _previousLongitude && phi == _previousPhi) {
        throw new RuntimeException("Polygon is not valid: it has two identical consecutive vertices");
      }

      double deltaLongitude = longitude - _previousLongitude;
      _sphericalExcess += 2 * Math.atan2(Math.tan(deltaLongitude / 2) * (_previousTan + tan), 1 + _previousTan * tan);

      double cos = Math.cos(phi);
      double sin = Math.sin(phi);
      double sinOfDeltaLongitude = Math.sin(deltaLongitude);
      double cosOfDeltaLongitude = Math.cos(deltaLongitude);

      // Initial bearing from previous to current
      double y = sinOfDeltaLongitude * cos;
      double x = _previousCos * sin - _previousSin * cos * cosOfDeltaLongitude;
      double initialBearing = (Math.atan2(y, x) + TWO_PI) % TWO_PI;

      // Final bearing from previous to current = opposite of bearing from current to previous
      double finalY = -sinOfDeltaLongitude * _previousCos;
      double finalX = _previousSin * cos - _previousCos * sin * cosOfDeltaLongitude;
      double finalBearing = (Math.atan2(finalY, finalX) + PI) % TWO_PI;

      // When processing our first point we don't yet have a _previousFinalBearing
      if (_firstPoint) {
        // So keep our initial bearing around, and we'll use it at the end
        // with the last final bearing
        _firstInitialBearing = initialBearing;
        _firstPoint = false;
      } else {
        _courseDelta += (initialBearing - _previousFinalBearing + THREE_PI) % TWO_PI - PI;
      }

      _courseDelta += (finalBearing - initialBearing + THREE_PI) % TWO_PI - PI;

      _previousFinalBearing = finalBearing;
      _previousCos = cos;
      _previousSin = sin;
      _previousPhi = phi;
      _previousTan = tan;
      _previousLongitude = longitude;
    }

    public double computeSphericalExcess() {
      if (!_done) {
        // Now that we have our last final bearing, we can calculate the remaining course delta
        _courseDelta += (_firstInitialBearing - _previousFinalBearing + THREE_PI) % TWO_PI - PI;

        // The courseDelta should be 2Pi or - 2Pi, unless a pole is enclosed (and then it should be ~ 0)
        // In which case we need to correct the spherical excess by 2Pi
        if (Math.abs(_courseDelta) < PI / 4) {
          _sphericalExcess = Math.abs(_sphericalExcess) - TWO_PI;
        }
        _done = true;
      }

      return _sphericalExcess;
    }
  }
}
