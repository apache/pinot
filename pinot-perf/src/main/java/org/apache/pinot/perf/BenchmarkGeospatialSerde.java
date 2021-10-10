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
package org.apache.pinot.perf;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;


@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 3, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 4, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkGeospatialSerde {
  // POINT
  @Benchmark
  public Object serializePoint(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._point);
  }

  @Benchmark
  public Object deserializePoint(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._pointSerialized);
  }

  // MULTI POINT
  @Benchmark
  public Object serializeSimpleMultipoint(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._simpleMultipoint);
  }

  @Benchmark
  public Object deserializeSimpleMultipoint(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._simpleMultipointSerialized);
  }

  @Benchmark
  public Object serializeComplexMultipoint(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._complexMultipoint);
  }

  @Benchmark
  public Object deserializeComplexMultipoint(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._complexMultipointSerialized);
  }

  // LINE STRING
  @Benchmark
  public Object serializeSimpleLineString(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._simpleLineString);
  }

  @Benchmark
  public Object deserializeSimpleLineString(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._simpleLineStringSerialized);
  }

  @Benchmark
  public Object serializeComplexLineString(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._complexLineString);
  }

  @Benchmark
  public Object deserializeComplexLineString(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._complexLineStringSerialized);
  }

  // MULTILINE STRING
  @Benchmark
  public Object serializeSimpleMultiLineString(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._simpleMultiLineString);
  }

  @Benchmark
  public Object deserializeSimpleMultiLineString(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._simpleMultiLineStringSerialized);
  }

  @Benchmark
  public Object serializeComplexMultiLineString(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._complexMultiLineString);
  }

  @Benchmark
  public Object deserializeComplexMultiLineString(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._complexMultiLineStringSerialized);
  }

  // POLYGON
  @Benchmark
  public Object serializeSimplePolygon(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._simplePolygon);
  }

  @Benchmark
  public Object deserializeSimplePolygon(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._simplePolygonSerialized);
  }

  @Benchmark
  public Object serializeComplexPolygon(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._complexPolygon);
  }

  @Benchmark
  public Object deserializeComplexPolygon(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._complexPolygonSerialized);
  }

  // MULTI POLYGON
  @Benchmark
  public Object serializeSimpleMultiPolygon(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._simpleMultiPolygon);
  }

  @Benchmark
  public Object deserializeSimpleMultiPolygon(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._simpleMultiPolygonSerialized);
  }

  @Benchmark
  public Object serializeComplexMultiPolygon(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._complexMultiPolygon);
  }

  @Benchmark
  public Object deserializeComplexMultiPolygon(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._complexMultiPolygonSerialized);
  }

  // GEOMETRY COLLECTION
  @Benchmark
  public Object serializeSimpleGeometryCollection(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._simpleGeometryCollection);
  }

  @Benchmark
  public Object deserializeSimpleGeometryCollection(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._simpleGeometryCollectionSerialized);
  }

  @Benchmark
  public Object serializeComplexGeometryCollection(BenchmarkData data) {
    return ObjectSerDeUtils.serialize(data._complexGeometryCollection);
  }

  @Benchmark
  public Object deserializeComplexGeometryCollection(BenchmarkData data) {
    return GeometrySerializer.deserialize(data._complexGeometryCollectionSerialized);
  }

  @State(Scope.Thread)
  public static class BenchmarkData {
    // POINT
    private Geometry _point;
    private byte[] _pointSerialized;

    // MULTI POINT
    private Geometry _simpleMultipoint;
    private byte[] _simpleMultipointSerialized;
    private Geometry _complexMultipoint;
    private byte[] _complexMultipointSerialized;

    // LINE STRING
    private Geometry _simpleLineString;
    private byte[] _simpleLineStringSerialized;
    private Geometry _complexLineString;
    private byte[] _complexLineStringSerialized;

    // MULTILINE STRING
    private Geometry _simpleMultiLineString;
    private byte[] _simpleMultiLineStringSerialized;
    private Geometry _complexMultiLineString;
    private byte[] _complexMultiLineStringSerialized;

    // POLYGON
    private Geometry _simplePolygon;
    private byte[] _simplePolygonSerialized;
    private Geometry _complexPolygon;
    private byte[] _complexPolygonSerialized;

    // MULTI POLYGON
    private Geometry _simpleMultiPolygon;
    private byte[] _simpleMultiPolygonSerialized;
    private Geometry _complexMultiPolygon;
    private byte[] _complexMultiPolygonSerialized;

    // COLLECTION
    private Geometry _simpleGeometryCollection;
    private byte[] _simpleGeometryCollectionSerialized;
    private Geometry _complexGeometryCollection;
    private byte[] _complexGeometryCollectionSerialized;

    @Setup
    public void setup() {
      _point = fromText(BenchmarkResource.POINT);
      _pointSerialized = ObjectSerDeUtils.serialize(_point);

      _simpleMultipoint = fromText(BenchmarkResource.MULTIPOINT);
      _simpleMultipointSerialized = ObjectSerDeUtils.serialize(_simpleMultipoint);
      _complexMultipoint = fromText(BenchmarkResource.readResource("geospatial/complex-multipoint.txt"));
      _complexMultipointSerialized = ObjectSerDeUtils.serialize(_complexMultipoint);

      _simpleLineString = fromText(BenchmarkResource.LINESTRING);
      _simpleLineStringSerialized = ObjectSerDeUtils.serialize(_simpleLineString);
      _complexLineString = fromText(BenchmarkResource.readResource("geospatial/complex-linestring.txt"));
      _complexLineStringSerialized = ObjectSerDeUtils.serialize(_complexLineString);

      _simpleMultiLineString = fromText(BenchmarkResource.MULTILINESTRING);
      _simpleMultiLineStringSerialized = ObjectSerDeUtils.serialize(_simpleMultiLineString);
      _complexMultiLineString = fromText(BenchmarkResource.readResource("geospatial/complex-multilinestring.txt"));
      _complexMultiLineStringSerialized = ObjectSerDeUtils.serialize(_complexMultiLineString);

      _simplePolygon = fromText(BenchmarkResource.POLYGON);
      _simplePolygonSerialized = ObjectSerDeUtils.serialize(_simplePolygon);
      _complexPolygon = fromText(BenchmarkResource.readResource("geospatial/complex-polygon.txt"));
      _complexPolygonSerialized = ObjectSerDeUtils.serialize(_complexPolygon);

      _simpleMultiPolygon = fromText(BenchmarkResource.MULTIPOLYGON);
      _simpleMultiPolygonSerialized = ObjectSerDeUtils.serialize(_simpleMultiPolygon);
      _complexMultiPolygon = fromText(BenchmarkResource.readResource("geospatial/complex-multipolygon.txt"));
      _complexMultiPolygonSerialized = ObjectSerDeUtils.serialize(_complexMultiPolygon);

      _simpleGeometryCollection = fromText(BenchmarkResource.GEOMETRYCOLLECTION);
      _simpleGeometryCollectionSerialized = ObjectSerDeUtils.serialize(_simpleGeometryCollection);
      _complexGeometryCollection = fromText("GEOMETRYCOLLECTION (" + Joiner.on(", ")
          .join(BenchmarkResource.readResource("geospatial/complex-multipoint.txt"),
              BenchmarkResource.readResource("geospatial/complex-linestring.txt"),
              BenchmarkResource.readResource("geospatial/complex-multilinestring.txt"),
              BenchmarkResource.readResource("geospatial/complex-polygon.txt"),
              BenchmarkResource.readResource("geospatial/complex-multipolygon.txt")) + ")");
      _complexGeometryCollectionSerialized = ObjectSerDeUtils.serialize(_complexGeometryCollection);
    }
  }

  public static class BenchmarkResource {
    public static final String POINT = "POINT (-2e3 -4e33)";
    public static final String MULTIPOINT = "MULTIPOINT (-2e3 -4e33, 0 0, 1 1, 2 3)";
    public static final String LINESTRING = "LINESTRING (-2e3 -4e33, 0 0, 1 1, 2 3)";
    public static final String MULTILINESTRING =
        "MULTILINESTRING ((-2e3 -4e33, 0 0, 1 1, 2 3), (0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7))";
    public static final String POLYGON = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
    public static final String MULTIPOLYGON =
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 10, 40 40, 20 40, 10 "
            + "20, 30 10)))";
    public static final String GEOMETRYCOLLECTION =
        "GEOMETRYCOLLECTION (" + "POINT (-2e3 -4e33), " + "MULTIPOINT (-2e3 -4e33, 0 0, 1 1, 2 3), "
            + "LINESTRING (-2e3 -4e33, 0 0, 1 1, 2 3), "
            + "MULTILINESTRING ((-2e3 -4e33, 0 0, 1 1, 2 3), (0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7)), "
            + "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)), "
            + "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 10, 40 40, 20 "
            + "40, 10 20, 30 10))))";

    public static String readResource(String resource) {
      try {
        return Resources.toString(Resources.getResource(resource), UTF_8);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static Geometry fromText(String text) {
    try {
      return new WKTReader().read(text);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args)
      throws RunnerException {
    Options options =
        new OptionsBuilder().verbosity(VerboseMode.NORMAL).include(".*" + BenchmarkData.class.getSimpleName() + ".*")
            .build();
    new Runner(options).run();
  }
}
