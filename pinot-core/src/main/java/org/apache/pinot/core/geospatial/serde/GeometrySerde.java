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
package org.apache.pinot.core.geospatial.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.core.geospatial.GeometryUtils.GEOGRAPHY_FACTORY;
import static org.apache.pinot.core.geospatial.GeometryUtils.GEOGRAPHY_GET_MASK;
import static org.apache.pinot.core.geospatial.GeometryUtils.GEOGRAPHY_SET_MASK;
import static org.apache.pinot.core.geospatial.GeometryUtils.GEOGRAPHY_SRID;
import static org.apache.pinot.core.geospatial.GeometryUtils.GEOMETRY_FACTORY;

/**
 * Provides methods to efficiently serialize and deserialize geometry types.
 */
public class GeometrySerde extends Serializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeometrySerde.class);



    @Override
    public void write(Kryo kryo, Output output, Object object) {
        if (!(object instanceof Geometry)) {
            throw new UnsupportedOperationException("Cannot serialize object of type " +
                    object.getClass().getName());
        }
        writeGeometry(output, (Geometry) object);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeByte = input.readByte();
        GeometrySerializationType type = readGeometryType(typeByte);
        GeometryFactory factory = getGeometryFactory(typeByte);

        return readGeometry(input, type, factory);
    }

    private Geometry readGeometry(Input input, GeometrySerializationType type, GeometryFactory factory) {
        switch (type) {
            case POINT:
                return readPoint(input, factory);
            case MULTI_POINT:
                return readMultiPoint(input, factory);
            case LINE_STRING:
                return readPolyline(input, false, factory);
            case MULTI_LINE_STRING:
                return readPolyline(input, true, factory);
            case POLYGON:
                return readPolygon(input, false, factory);
            case MULTI_POLYGON:
                return readPolygon(input, true, factory);
            case GEOMETRY_COLLECTION:
                return readGeometryCollection(input, factory);
            //            case ENVELOPE:
            //                return readEnvelope(input);
            default:
                throw new UnsupportedOperationException("Unexpected type: " + type);
        }
    }

    private Point readPoint(Input input, GeometryFactory factory) {
        Coordinate coordinates = readCoordinate(input);
        if (isNaN(coordinates.x) || isNaN(coordinates.y)) {
            return factory.createPoint();
        }
        return factory.createPoint(coordinates);
    }

    private Coordinate readCoordinate(Input input) {
        return new Coordinate(input.readDouble(), input.readDouble());
    }

    private Coordinate[] readCoordinates(Input input, int count) {
        requireNonNull(input, "input is null");
        verify(count > 0);
        Coordinate[] coordinates = new Coordinate[count];
        for (int i = 0; i < count; i++) {
            coordinates[i] = readCoordinate(input);
        }
        return coordinates;
    }

    private Geometry readMultiPoint(Input input, GeometryFactory factory) {
        int pointCount = input.readInt();
        Point[] points = new Point[pointCount];
        for (int i = 0; i < pointCount; i++) {
            points[i] = readPoint(input, factory);
        }
        return factory.createMultiPoint(points);
    }

    private GeometrySerializationType readGeometryType(byte typeByte) {
        return GeometrySerializationType.fromID(typeByte & GEOGRAPHY_GET_MASK);
    }

    private Geometry readPolyline(Input input, boolean multitype, GeometryFactory factory) {
        int partCount = input.readInt();
        if (partCount == 0) {
            if (multitype) {
                return factory.createMultiLineString();
            }
            return factory.createLineString();
        }

        int pointCount = input.readInt();
        int[] startIndexes = new int[partCount];
        for (int i = 0; i < partCount; i++) {
            startIndexes[i] = input.readInt();
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
            lineStrings[i] = factory.createLineString(readCoordinates(input, partLengths[i]));
        }

        if (multitype) {
            return factory.createMultiLineString(lineStrings);
        }
        verify(lineStrings.length == 1);
        return lineStrings[0];
    }

    private Geometry readPolygon(Input input, boolean multitype, GeometryFactory factory) {
        int partCount = input.readInt();
        if (partCount == 0) {
            if (multitype) {
                return factory.createMultiPolygon();
            }
            return factory.createPolygon();
        }

        int pointCount = input.readInt();
        int[] startIndexes = new int[partCount];
        for (int i = 0; i < partCount; i++) {
            startIndexes[i] = input.readInt();
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
                Coordinate[] coordinates = readCoordinates(input, partLengths[i]);
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
        return getOnlyElement(polygons);
    }

    private Geometry readGeometryCollection(Input input, GeometryFactory factory) {
        List<Geometry> geometries = new ArrayList<>();
        while (true) {
            try {
                if (!(input.available() > 0)) break;
            } catch (IOException e) {
                throw new IllegalArgumentException("Corrupted data: failed to read geometry collection.");
            }
            byte typeByte = input.readByte();
            GeometrySerializationType type = readGeometryType(typeByte);
            GeometryFactory geometryFactory = getGeometryFactory(typeByte);
            geometries.add(readGeometry(input, type, geometryFactory));
        }
        return factory.createGeometryCollection(geometries.toArray(new Geometry[0]));
    }

    private boolean isClockwise(Coordinate[] coordinates) {
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


    private GeometryFactory getGeometryFactory(byte typeByte) {
        return typeByte < 0 ? GEOGRAPHY_FACTORY : GEOMETRY_FACTORY;
    }

    private void writeGeometry(Output output, Geometry geometry) {
        switch (geometry.getGeometryType()) {
            case "Point":
                writePoint(output, (Point) geometry);
                break;
            case "MultiPoint":
                writeMultiPoint(output, (MultiPoint) geometry);
                break;
            case "LineString":
            case "LinearRing":
                // LinearRings are a subclass of LineString
                writePolyline(output, geometry, false);
                break;
            case "MultiLineString":
                writePolyline(output, geometry, true);
                break;
            case "Polygon":
                writePolygon(output, geometry, false);
                break;
            case "MultiPolygon":
                writePolygon(output, geometry, true);
                break;
            case "GeometryCollection":
                writeGeometryCollection(output, geometry);
                break;
            default:
                throw new IllegalArgumentException("Unsupported geometry type : " + geometry.getGeometryType());
        }
    }

    private void writeType(Output output, GeometrySerializationType serializationType, int SRID) {
        byte type = Integer.valueOf(serializationType.id()).byteValue();
        if (SRID == GEOGRAPHY_SRID) {
            type |= GEOGRAPHY_SET_MASK;
        }
        output.writeByte(type);
    }

    private void writePoint(Output output, Point point) {
        writeType(output, GeometrySerializationType.POINT, point.getSRID());
        if (point.isEmpty()) {
            output.writeDouble(NaN);
            output.writeDouble(NaN);
        } else {
            writeCoordinate(output, point.getCoordinate());
        }
    }

    private void writeCoordinate(Output output, Coordinate coordinate) {
        output.writeDouble(coordinate.getX());
        output.writeDouble(coordinate.getY());
    }

    private void writeMultiPoint(Output output, MultiPoint geometry) {
        writeType(output, GeometrySerializationType.MULTI_POINT, geometry.getSRID());
        output.writeInt(geometry.getNumPoints());
        for (Coordinate coordinate : geometry.getCoordinates()) {
            writeCoordinate(output, coordinate);
        }
    }

    private void writePolyline(Output output, Geometry geometry, boolean multitype) {
        int numParts;
        int numPoints = geometry.getNumPoints();
        if (multitype) {
            numParts = geometry.getNumGeometries();
            writeType(output, GeometrySerializationType.MULTI_LINE_STRING, geometry.getSRID());
        } else {
            numParts = numPoints > 0 ? 1 : 0;
            writeType(output, GeometrySerializationType.LINE_STRING, geometry.getSRID());
        }
        output.writeInt(numParts);
        output.writeInt(numPoints);

        int partIndex = 0;
        for (int i = 0; i < numParts; i++) {
            output.writeInt(partIndex);
            partIndex += geometry.getGeometryN(i).getNumPoints();
        }

        writeCoordinates(output, geometry.getCoordinates());
    }

    private void writeCoordinates(Output output, Coordinate[] coordinates) {
        for (Coordinate coordinate : coordinates) {
            writeCoordinate(output, coordinate);
        }
    }

    private void writePolygon(Output output, Geometry geometry, boolean multitype) {
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
            writeType(output, GeometrySerializationType.MULTI_POLYGON, geometry.getSRID());
        } else {
            writeType(output, GeometrySerializationType.POLYGON, geometry.getSRID());
        }

        output.writeInt(numParts);
        output.writeInt(numPoints);

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
            output.writeInt(partIndex);
        }

        Coordinate[] coordinates = geometry.getCoordinates();
        canonicalizePolygonCoordinates(coordinates, partIndexes, shellPart);
        writeCoordinates(output, coordinates);
    }

    private void canonicalizePolygonCoordinates(Coordinate[] coordinates, int[] partIndexes, boolean[] shellPart) {
        for (int part = 0; part < partIndexes.length - 1; part++) {
            canonicalizePolygonCoordinates(coordinates, partIndexes[part], partIndexes[part + 1], shellPart[part]);
        }
        if (partIndexes.length > 0) {
            canonicalizePolygonCoordinates(coordinates, partIndexes[partIndexes.length - 1], coordinates.length,
                    shellPart[partIndexes.length - 1]);
        }
    }

    private void canonicalizePolygonCoordinates(Coordinate[] coordinates, int start, int end, boolean isShell) {
        boolean isClockwise = isClockwise(coordinates, start, end);

        if ((isShell && !isClockwise) || (!isShell && isClockwise)) {
            // shell has to be counter clockwise
            reverse(coordinates, start, end);
        }
    }

    private void reverse(Coordinate[] coordinates, int start, int end) {
        verify(start <= end, "start must be less or equal than end");
        for (int i = start; i < start + ((end - start) / 2); i++) {
            Coordinate buffer = coordinates[i];
            coordinates[i] = coordinates[start + end - i - 1];
            coordinates[start + end - i - 1] = buffer;
        }
    }

    private void writeGeometryCollection(Output output, Geometry collection) {
        writeType(output, GeometrySerializationType.GEOMETRY_COLLECTION, collection.getSRID());
        for (int geometryIndex = 0; geometryIndex < collection.getNumGeometries(); geometryIndex++) {
            Geometry geometry = collection.getGeometryN(geometryIndex);
            writeGeometry(output, geometry);
        }
    }

    private void writeEnvelope(Output output, Geometry geometry) {
        if (geometry.isEmpty()) {
            for (int i = 0; i < 4; i++) {
                output.writeDouble(NaN);
            }
            return;
        }

        Envelope envelope = geometry.getEnvelopeInternal();
        output.writeDouble(envelope.getMinX());
        output.writeDouble(envelope.getMinY());
        output.writeDouble(envelope.getMaxX());
        output.writeDouble(envelope.getMaxY());
    }
}
