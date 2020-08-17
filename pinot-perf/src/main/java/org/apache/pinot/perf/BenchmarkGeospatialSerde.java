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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
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

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;

import static org.apache.pinot.core.geospatial.serde.GeometrySerializer.serialize;
import static org.apache.pinot.core.geospatial.serde.GeometrySerializer.deserialize;


@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 3, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 4, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
@BenchmarkMode(Throughput)
public class BenchmarkGeospatialSerde {
  // POINT
  @Benchmark
  public Object serializePoint(BenchmarkData data) {
    return serialize(data.point);
  }

  @Benchmark
  public Object deserializePoint(BenchmarkData data) {
    return deserialize(data.pointSerialized);
  }

  // MULTI POINT
  @Benchmark
  public Object serializeSimpleMultipoint(BenchmarkData data) {
    return serialize(data.simpleMultipoint);
  }

  @Benchmark
  public Object deserializeSimpleMultipoint(BenchmarkData data) {
    return deserialize(data.simpleMultipointSerialized);
  }

  @Benchmark
  public Object serializeComplexMultipoint(BenchmarkData data) {
    return serialize(data.complexMultipoint);
  }

  @Benchmark
  public Object deserializeComplexMultipoint(BenchmarkData data) {
    return deserialize(data.complexMultipointSerialized);
  }

  // LINE STRING
  @Benchmark
  public Object serializeSimpleLineString(BenchmarkData data) {
    return serialize(data.simpleLineString);
  }

  @Benchmark
  public Object deserializeSimpleLineString(BenchmarkData data) {
    return deserialize(data.simpleLineStringSerialized);
  }

  @Benchmark
  public Object serializeComplexLineString(BenchmarkData data) {
    return serialize(data.complexLineString);
  }

  @Benchmark
  public Object deserializeComplexLineString(BenchmarkData data) {
    return deserialize(data.complexLineStringSerialized);
  }

  // MULTILINE STRING
  @Benchmark
  public Object serializeSimpleMultiLineString(BenchmarkData data) {
    return serialize(data.simpleMultiLineString);
  }

  @Benchmark
  public Object deserializeSimpleMultiLineString(BenchmarkData data) {
    return deserialize(data.simpleMultiLineStringSerialized);
  }

  @Benchmark
  public Object serializeComplexMultiLineString(BenchmarkData data) {
    return serialize(data.complexMultiLineString);
  }

  @Benchmark
  public Object deserializeComplexMultiLineString(BenchmarkData data) {
    return deserialize(data.complexMultiLineStringSerialized);
  }

  // POLYGON
  @Benchmark
  public Object serializeSimplePolygon(BenchmarkData data) {
    return serialize(data.simplePolygon);
  }

  @Benchmark
  public Object deserializeSimplePolygon(BenchmarkData data) {
    return deserialize(data.simplePolygonSerialized);
  }

  @Benchmark
  public Object serializeComplexPolygon(BenchmarkData data) {
    return serialize(data.complexPolygon);
  }

  @Benchmark
  public Object deserializeComplexPolygon(BenchmarkData data) {
    return deserialize(data.complexPolygonSerialized);
  }

  // MULTI POLYGON
  @Benchmark
  public Object serializeSimpleMultiPolygon(BenchmarkData data) {
    return serialize(data.simpleMultiPolygon);
  }

  @Benchmark
  public Object deserializeSimpleMultiPolygon(BenchmarkData data) {
    return deserialize(data.simpleMultiPolygonSerialized);
  }

  @Benchmark
  public Object serializeComplexMultiPolygon(BenchmarkData data) {
    return serialize(data.complexMultiPolygon);
  }

  @Benchmark
  public Object deserializeComplexMultiPolygon(BenchmarkData data) {
    return deserialize(data.complexMultiPolygonSerialized);
  }

  // GEOMETRY COLLECTION
  @Benchmark
  public Object serializeSimpleGeometryCollection(BenchmarkData data) {
    return serialize(data.simpleGeometryCollection);
  }

  @Benchmark
  public Object deserializeSimpleGeometryCollection(BenchmarkData data) {
    return deserialize(data.simpleGeometryCollectionSerialized);
  }

  @Benchmark
  public Object serializeComplexGeometryCollection(BenchmarkData data) {
    return serialize(data.complexGeometryCollection);
  }

  @Benchmark
  public Object deserializeComplexGeometryCollection(BenchmarkData data) {
    return deserialize(data.complexGeometryCollectionSerialized);
  }

  @State(Scope.Thread)
  public static class BenchmarkData {
    // POINT
    private Geometry point;
    private byte[] pointSerialized;

    // MULTI POINT
    private Geometry simpleMultipoint;
    private byte[] simpleMultipointSerialized;
    private Geometry complexMultipoint;
    private byte[] complexMultipointSerialized;

    // LINE STRING
    private Geometry simpleLineString;
    private byte[] simpleLineStringSerialized;
    private Geometry complexLineString;
    private byte[] complexLineStringSerialized;

    // MULTILINE STRING
    private Geometry simpleMultiLineString;
    private byte[] simpleMultiLineStringSerialized;
    private Geometry complexMultiLineString;
    private byte[] complexMultiLineStringSerialized;

    // POLYGON
    private Geometry simplePolygon;
    private byte[] simplePolygonSerialized;
    private Geometry complexPolygon;
    private byte[] complexPolygonSerialized;

    // MULTI POLYGON
    private Geometry simpleMultiPolygon;
    private byte[] simpleMultiPolygonSerialized;
    private Geometry complexMultiPolygon;
    private byte[] complexMultiPolygonSerialized;

    // COLLECTION
    private Geometry simpleGeometryCollection;
    private byte[] simpleGeometryCollectionSerialized;
    private Geometry complexGeometryCollection;
    private byte[] complexGeometryCollectionSerialized;

    @Setup
    public void setup() {
      point = fromText(BenchmarkResource.POINT);
      pointSerialized = serialize(point);

      simpleMultipoint = fromText(BenchmarkResource.MULTIPOINT);
      simpleMultipointSerialized = serialize(simpleMultipoint);
      complexMultipoint = fromText(BenchmarkResource.readResource("geospatial/complex-multipoint.txt"));
      complexMultipointSerialized = serialize(complexMultipoint);

      simpleLineString = fromText(BenchmarkResource.LINESTRING);
      simpleLineStringSerialized = serialize(simpleLineString);
      complexLineString = fromText(BenchmarkResource.readResource("geospatial/complex-linestring.txt"));
      complexLineStringSerialized = serialize(complexLineString);

      simpleMultiLineString = fromText(BenchmarkResource.MULTILINESTRING);
      simpleMultiLineStringSerialized = serialize(simpleMultiLineString);
      complexMultiLineString = fromText(BenchmarkResource.readResource("geospatial/complex-multilinestring.txt"));
      complexMultiLineStringSerialized = serialize(complexMultiLineString);

      simplePolygon = fromText(BenchmarkResource.POLYGON);
      simplePolygonSerialized = serialize(simplePolygon);
      complexPolygon = fromText(BenchmarkResource.readResource("geospatial/complex-polygon.txt"));
      complexPolygonSerialized = serialize(complexPolygon);

      simpleMultiPolygon = fromText(BenchmarkResource.MULTIPOLYGON);
      simpleMultiPolygonSerialized = serialize(simpleMultiPolygon);
      complexMultiPolygon = fromText(BenchmarkResource.readResource("geospatial/complex-multipolygon.txt"));
      complexMultiPolygonSerialized = serialize(complexMultiPolygon);

      simpleGeometryCollection = fromText(BenchmarkResource.GEOMETRYCOLLECTION);
      simpleGeometryCollectionSerialized = serialize(simpleGeometryCollection);
      complexGeometryCollection = fromText("GEOMETRYCOLLECTION (" + Joiner.on(", ")
          .join(BenchmarkResource.readResource("geospatial/complex-multipoint.txt"),
              BenchmarkResource.readResource("geospatial/complex-linestring.txt"),
              BenchmarkResource.readResource("geospatial/complex-multilinestring.txt"),
              BenchmarkResource.readResource("geospatial/complex-polygon.txt"),
              BenchmarkResource.readResource("geospatial/complex-multipolygon.txt")) + ")");
      complexGeometryCollectionSerialized = serialize(complexGeometryCollection);
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
        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 10, 40 40, 20 40, 10 20, 30 10)))";
    public static final String GEOMETRYCOLLECTION =
        "GEOMETRYCOLLECTION (" + "POINT (-2e3 -4e33), " + "MULTIPOINT (-2e3 -4e33, 0 0, 1 1, 2 3), "
            + "LINESTRING (-2e3 -4e33, 0 0, 1 1, 2 3), "
            + "MULTILINESTRING ((-2e3 -4e33, 0 0, 1 1, 2 3), (0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7)), "
            + "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)), "
            + "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)), ((30 10, 40 40, 20 40, 10 20, 30 10))))";

    public static String readResource(String resource) {
      try {
        return Resources.toString(getResource(resource), UTF_8);
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


