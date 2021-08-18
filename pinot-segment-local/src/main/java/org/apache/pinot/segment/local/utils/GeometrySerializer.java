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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;

import static org.apache.pinot.segment.local.utils.GeometryType.*;


/**
 * Provides methods to efficiently serialize and deserialize geometry types.
 *
 * This serialization is similar to Presto's https://github
 * .com/prestodb/presto/blob/master/presto-geospatial-toolkit/src/main/java/com/facebook/presto/geospatial/serde/JtsGeometrySerde.java,
 * with the following differences:
 *  - The geometry vs geography info is encoded in the type byte.
 *  - The envelope info is not serialized
 */
public final class GeometrySerializer {
  private static final int TYPE_SIZE = Byte.BYTES;
  private static final int COORDINATE_SIZE = Double.BYTES + Double.BYTES;

  /**
   * Serializes a geometry object into bytes
   * @param geometry the geometry object to serialize
   * @return the serialized bytes
   */
  public static byte[] serialize(Geometry geometry) {
    return writeGeometry(geometry);
  }

  /**
   * Deserializes bytes into a geometry object
   * @param bytes the bytes to deserialize
   * @return the deserialized object
   */
  public static Geometry deserialize(byte[] bytes) {
    return readGeometry(bytes);
  }

  private static byte[] writeGeometry(Geometry geometry) {
    byte[] bytes = new byte[getByteSize(geometry)];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    writeGeometryByteBuffer(byteBuffer, geometry);
    return bytes;
  }

  private static Geometry readGeometry(byte[] bytes) {
    return readGeometry(ByteBuffer.wrap(bytes));
  }

  private static Geometry readGeometry(ByteBuffer byteBuffer) {
    byte typeByte = byteBuffer.get();
    GeometryType type = readGeometryType(typeByte);
    GeometryFactory factory = getGeometryFactory(typeByte);
    Geometry geometry = readGeometry(byteBuffer, type, factory);
    return geometry;
  }

  private static Geometry readGeometry(ByteBuffer byteBuffer, GeometryType type, GeometryFactory factory) {
    switch (type) {
      case POINT:
        return readPoint(byteBuffer, factory);
      case MULTI_POINT:
        return readMultiPoint(byteBuffer, factory);
      case LINE_STRING:
        return readPolyline(byteBuffer, false, factory);
      case MULTI_LINE_STRING:
        return readPolyline(byteBuffer, true, factory);
      case POLYGON:
        return readPolygon(byteBuffer, false, factory);
      case MULTI_POLYGON:
        return readPolygon(byteBuffer, true, factory);
      case GEOMETRY_COLLECTION:
        return readGeometryCollection(byteBuffer, factory);
      default:
        throw new UnsupportedOperationException("Unexpected type: " + type);
    }
  }

  private static Point readPoint(ByteBuffer byteBuffer, GeometryFactory factory) {
    Coordinate coordinates = readCoordinate(byteBuffer);
    if (Double.isNaN(coordinates.x) || Double.isNaN(coordinates.y)) {
      return factory.createPoint();
    }
    return factory.createPoint(coordinates);
  }

  private static Coordinate readCoordinate(ByteBuffer byteBuffer) {
    return new Coordinate(byteBuffer.getDouble(), byteBuffer.getDouble());
  }

  private static Coordinate[] readCoordinates(ByteBuffer byteBuffer, int count) {
    Preconditions.checkArgument(count > 0, "Count shall be positive");
    Coordinate[] coordinates = new Coordinate[count];
    for (int i = 0; i < count; i++) {
      coordinates[i] = readCoordinate(byteBuffer);
    }
    return coordinates;
  }

  private static Geometry readMultiPoint(ByteBuffer byteBuffer, GeometryFactory factory) {
    int pointCount = byteBuffer.getInt();
    Point[] points = new Point[pointCount];
    for (int i = 0; i < pointCount; i++) {
      points[i] = readPoint(byteBuffer, factory);
    }
    return factory.createMultiPoint(points);
  }

  private static GeometryType readGeometryType(byte typeByte) {
    return fromID(typeByte & GeometryUtils.GEOGRAPHY_GET_MASK);
  }

  private static Geometry readPolyline(ByteBuffer byteBuffer, boolean multitype, GeometryFactory factory) {
    int partCount = byteBuffer.getInt();
    if (partCount == 0) {
      if (multitype) {
        return factory.createMultiLineString();
      }
      return factory.createLineString();
    }

    int pointCount = byteBuffer.getInt();
    int[] startIndexes = new int[partCount];
    for (int i = 0; i < partCount; i++) {
      startIndexes[i] = byteBuffer.getInt();
    }

    int[] partLengths = new int[partCount];
    if (partCount > 1) {
      partLengths[0] = startIndexes[1];
      for (int i = 1; i < partCount - 1; i++) {
        partLengths[i] = startIndexes[i + 1] - startIndexes[i];
      }
    }
    partLengths[partCount - 1] = pointCount - startIndexes[partCount - 1];

    LineString[] lineStrings = new LineString[partCount];

    for (int i = 0; i < partCount; i++) {
      lineStrings[i] = factory.createLineString(readCoordinates(byteBuffer, partLengths[i]));
    }

    if (multitype) {
      return factory.createMultiLineString(lineStrings);
    }
    Preconditions.checkArgument(lineStrings.length == 1, "The remaining line string must have only one node");
    return lineStrings[0];
  }

  private static Geometry readPolygon(ByteBuffer byteBuffer, boolean multitype, GeometryFactory factory) {
    int partCount = byteBuffer.getInt();
    if (partCount == 0) {
      if (multitype) {
        return factory.createMultiPolygon();
      }
      return factory.createPolygon();
    }

    int pointCount = byteBuffer.getInt();
    int[] startIndexes = new int[partCount];
    for (int i = 0; i < partCount; i++) {
      startIndexes[i] = byteBuffer.getInt();
    }

    int[] partLengths = new int[partCount];
    if (partCount > 1) {
      partLengths[0] = startIndexes[1];
      for (int i = 1; i < partCount - 1; i++) {
        partLengths[i] = startIndexes[i + 1] - startIndexes[i];
      }
    }
    partLengths[partCount - 1] = pointCount - startIndexes[partCount - 1];

    LinearRing shell = null;
    List<LinearRing> holes = new ArrayList<>();
    List<Polygon> polygons = new ArrayList<>();
    try {
      for (int i = 0; i < partCount; i++) {
        Coordinate[] coordinates = readCoordinates(byteBuffer, partLengths[i]);
        if (isClockwise(coordinates)) {
          // next polygon has started
          if (shell != null) {
            polygons.add(factory.createPolygon(shell, holes.toArray(new LinearRing[0])));
            holes.clear();
          }
          shell = factory.createLinearRing(coordinates);
        } else {
          holes.add(factory.createLinearRing(coordinates));
        }
      }
      polygons.add(factory.createPolygon(shell, holes.toArray(new LinearRing[0])));
    } catch (IllegalArgumentException e) {
      throw new TopologyException("Error constructing Polygon: " + e.getMessage());
    }

    if (multitype) {
      return factory.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }
    return Iterables.getOnlyElement(polygons);
  }

  private static Geometry readGeometryCollection(ByteBuffer byteBuffer, GeometryFactory factory) {
    List<Geometry> geometries = new ArrayList<>();
    while (true) {
      if (!byteBuffer.hasRemaining()) {
        break;
      }
      byte typeByte = byteBuffer.get();
      GeometryType type = readGeometryType(typeByte);
      GeometryFactory geometryFactory = getGeometryFactory(typeByte);
      geometries.add(readGeometry(byteBuffer, type, geometryFactory));
    }
    return factory.createGeometryCollection(geometries.toArray(new Geometry[0]));
  }

  private static boolean isClockwise(Coordinate[] coordinates) {
    return isClockwise(coordinates, 0, coordinates.length);
  }

  private static boolean isClockwise(Coordinate[] coordinates, int start, int end) {
    // Sum over the edges: (x2 − x1) * (y2 + y1).
    // If the result is positive the curve is clockwise,
    // if it's negative the curve is counter-clockwise.
    double area = 0;
    for (int i = start + 1; i < end; i++) {
      area += (coordinates[i].x - coordinates[i - 1].x) * (coordinates[i].y + coordinates[i - 1].y);
    }
    area += (coordinates[start].x - coordinates[end - 1].x) * (coordinates[start].y + coordinates[end - 1].y);
    return area > 0;
  }

  private static GeometryFactory getGeometryFactory(byte typeByte) {
    return typeByte < 0 ? GeometryUtils.GEOGRAPHY_FACTORY : GeometryUtils.GEOMETRY_FACTORY;
  }

  private static void writeGeometryByteBuffer(ByteBuffer byteBuffer, Geometry geometry) {
    switch (geometry.getGeometryType()) {
      case "Point":
        writePoint(byteBuffer, (Point) geometry);
        break;
      case "MultiPoint":
        writeMultiPoint(byteBuffer, (MultiPoint) geometry);
        break;
      case "LineString":
      case "LinearRing":
        // LinearRings are a subclass of LineString
        writePolyline(byteBuffer, geometry, false);
        break;
      case "MultiLineString":
        writePolyline(byteBuffer, geometry, true);
        break;
      case "Polygon":
        writePolygon(byteBuffer, geometry, false);
        break;
      case "MultiPolygon":
        writePolygon(byteBuffer, geometry, true);
        break;
      case "GeometryCollection":
        writeGeometryCollection(byteBuffer, geometry);
        break;
      default:
        throw new IllegalArgumentException("Unsupported geometry type : " + geometry.getGeometryType());
    }
  }

  private static int getByteSize(Geometry geometry) {
    int size = TYPE_SIZE;
    switch (geometry.getGeometryType()) {
      case "Point":
        size += COORDINATE_SIZE;
        break;
      case "MultiPoint":
        size += Integer.BYTES + geometry.getNumPoints() * COORDINATE_SIZE;
        break;
      case "LineString":
      case "LinearRing":
        // LinearRings are a subclass of LineString
        size += getPolylineByteSize(geometry, false);
        break;
      case "MultiLineString":
        size += getPolylineByteSize(geometry, true);
        break;
      case "Polygon":
        size += getPolygonByteSize(geometry, false);
        break;
      case "MultiPolygon":
        size += getPolygonByteSize(geometry, true);
        break;
      case "GeometryCollection":
        size += getGeometryCollectionByteSize(geometry);
        break;
      default:
        throw new IllegalArgumentException("Unsupported geometry type : " + geometry.getGeometryType());
    }
    return size;
  }

  private static void writeType(ByteBuffer byteBuffer, GeometryType serializationType, int srid) {
    byte type = Integer.valueOf(serializationType.id()).byteValue();
    if (srid == GeometryUtils.GEOGRAPHY_SRID) {
      type |= GeometryUtils.GEOGRAPHY_SET_MASK;
    }
    byteBuffer.put(type);
  }

  /**
   * Writes the byte of type, followed by the two coordinates in double.
   */
  private static void writePoint(ByteBuffer byteBuffer, Point point) {
    writeType(byteBuffer, POINT, point.getSRID());
    if (point.isEmpty()) {
      byteBuffer.putDouble(Double.NaN);
      byteBuffer.putDouble(Double.NaN);
    } else {
      writeCoordinate(byteBuffer, point.getCoordinate());
    }
  }

  private static void writeCoordinate(ByteBuffer byteBuffer, Coordinate coordinate) {
    byteBuffer.putDouble(coordinate.getX());
    byteBuffer.putDouble(coordinate.getY());
  }

  /**
   * Writes the byte of type, number of points in int, followed by the collection of points
   */
  private static void writeMultiPoint(ByteBuffer byteBuffer, MultiPoint geometry) {
    writeType(byteBuffer, MULTI_POINT, geometry.getSRID());
    byteBuffer.putInt(geometry.getNumPoints());
    for (Coordinate coordinate : geometry.getCoordinates()) {
      writeCoordinate(byteBuffer, coordinate);
    }
  }

  private static int getPolylineByteSize(Geometry geometry, boolean multitype) {
    int numPoints = geometry.getNumPoints();
    int numParts = multitype ? geometry.getNumGeometries() : numPoints > 0 ? 1 : 0;
    return Integer.BYTES + Integer.BYTES + numParts * Integer.BYTES + numPoints * COORDINATE_SIZE;
  }

  /**
   * Writes the byte of type, the number of parts in int, number of points in int, followed by collection of part index
   * in int, followed by collection of coordinates
   */
  private static void writePolyline(ByteBuffer byteBuffer, Geometry geometry, boolean multitype) {
    int numParts;
    int numPoints = geometry.getNumPoints();
    if (multitype) {
      numParts = geometry.getNumGeometries();
      writeType(byteBuffer, MULTI_LINE_STRING, geometry.getSRID());
    } else {
      numParts = numPoints > 0 ? 1 : 0;
      writeType(byteBuffer, LINE_STRING, geometry.getSRID());
    }
    byteBuffer.putInt(numParts);
    byteBuffer.putInt(numPoints);

    int partIndex = 0;
    for (int i = 0; i < numParts; i++) {
      byteBuffer.putInt(partIndex);
      partIndex += geometry.getGeometryN(i).getNumPoints();
    }

    writeCoordinates(byteBuffer, geometry.getCoordinates());
  }

  private static void writeCoordinates(ByteBuffer byteBuffer, Coordinate[] coordinates) {
    for (Coordinate coordinate : coordinates) {
      writeCoordinate(byteBuffer, coordinate);
    }
  }

  private static int getPolygonByteSize(Geometry geometry, boolean multitype) {
    int numGeometries = geometry.getNumGeometries();
    int numParts = 0;
    int numPoints = geometry.getNumPoints();
    for (int i = 0; i < numGeometries; i++) {
      Polygon polygon = (Polygon) geometry.getGeometryN(i);
      if (polygon.getNumPoints() > 0) {
        numParts += polygon.getNumInteriorRing() + 1;
      }
    }
    int size = Integer.BYTES + Integer.BYTES;
    if (numParts == 0) {
      return size;
    }
    return size + numParts * Integer.BYTES + numPoints * COORDINATE_SIZE;
  }

  /**
   * Writes the byte of type, the number of parts in int, number of points in int, followed by collection of part index
   * in int, followed by the canonicalized coordinates.
   */
  private static void writePolygon(ByteBuffer byteBuffer, Geometry geometry, boolean multitype) {
    int numGeometries = geometry.getNumGeometries();
    int numParts = 0;
    int numPoints = geometry.getNumPoints();
    for (int i = 0; i < numGeometries; i++) {
      Polygon polygon = (Polygon) geometry.getGeometryN(i);
      if (polygon.getNumPoints() > 0) {
        numParts += polygon.getNumInteriorRing() + 1;
      }
    }

    if (multitype) {
      writeType(byteBuffer, MULTI_POLYGON, geometry.getSRID());
    } else {
      writeType(byteBuffer, POLYGON, geometry.getSRID());
    }

    byteBuffer.putInt(numParts);
    byteBuffer.putInt(numPoints);

    if (numParts == 0) {
      return;
    }

    int[] partIndexes = new int[numParts];
    boolean[] shellPart = new boolean[numParts];

    int currentPart = 0;
    int currentPoint = 0;
    for (int i = 0; i < numGeometries; i++) {
      Polygon polygon = (Polygon) geometry.getGeometryN(i);

      partIndexes[currentPart] = currentPoint;
      shellPart[currentPart] = true;
      currentPart++;
      currentPoint += polygon.getExteriorRing().getNumPoints();

      int holesCount = polygon.getNumInteriorRing();
      for (int holeIndex = 0; holeIndex < holesCount; holeIndex++) {
        partIndexes[currentPart] = currentPoint;
        shellPart[currentPart] = false;
        currentPart++;
        currentPoint += polygon.getInteriorRingN(holeIndex).getNumPoints();
      }
    }

    for (int partIndex : partIndexes) {
      byteBuffer.putInt(partIndex);
    }

    Coordinate[] coordinates = geometry.getCoordinates();
    canonicalizePolygonCoordinates(coordinates, partIndexes, shellPart);
    writeCoordinates(byteBuffer, coordinates);
  }

  private static void canonicalizePolygonCoordinates(Coordinate[] coordinates, int[] partIndexes, boolean[] shellPart) {
    for (int part = 0; part < partIndexes.length - 1; part++) {
      canonicalizePolygonCoordinates(coordinates, partIndexes[part], partIndexes[part + 1], shellPart[part]);
    }
    if (partIndexes.length > 0) {
      canonicalizePolygonCoordinates(coordinates, partIndexes[partIndexes.length - 1], coordinates.length,
          shellPart[partIndexes.length - 1]);
    }
  }

  private static void canonicalizePolygonCoordinates(Coordinate[] coordinates, int start, int end, boolean isShell) {
    boolean isClockwise = isClockwise(coordinates, start, end);

    if ((isShell && !isClockwise) || (!isShell && isClockwise)) {
      // shell has to be counter clockwise
      reverse(coordinates, start, end);
    }
  }

  private static void reverse(Coordinate[] coordinates, int start, int end) {
    Preconditions.checkArgument(start <= end, "start must be less or equal than end");
    for (int i = start; i < start + ((end - start) / 2); i++) {
      Coordinate buffer = coordinates[i];
      coordinates[i] = coordinates[start + end - i - 1];
      coordinates[start + end - i - 1] = buffer;
    }
  }

  private static int getGeometryCollectionByteSize(Geometry collection) {
    int size = 0;
    for (int geometryIndex = 0; geometryIndex < collection.getNumGeometries(); geometryIndex++) {
      Geometry geometry = collection.getGeometryN(geometryIndex);
      size += getByteSize(geometry);
    }
    return size;
  }

  private static void writeGeometryCollection(ByteBuffer byteBuffer, Geometry collection) {
    writeType(byteBuffer, GEOMETRY_COLLECTION, collection.getSRID());
    for (int geometryIndex = 0; geometryIndex < collection.getNumGeometries(); geometryIndex++) {
      Geometry geometry = collection.getGeometryN(geometryIndex);
      writeGeometryByteBuffer(byteBuffer, geometry);
    }
  }

  private GeometrySerializer() {
  }
}
