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

import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKTReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.GeometryUtils.GEOGRAPHY_SRID;


public class GeometrySerdeTest {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final GeometryFactory GEOGRAPHY_FACTORY = new GeometryFactory(new PrecisionModel(), GEOGRAPHY_SRID);

  @Test
  public void testPoint()
      throws Exception {
    testSerde("POINT (1 2)");
    testSerde("POINT (-1 -2)");
    testSerde("POINT (0 0)");
    testSerde("POINT (-2e3 -4e33)");
    testSerde("POINT EMPTY");
  }

  @Test
  public void testMultiPoint()
      throws Exception{
    testSerde("MULTIPOINT (0 0)");
    testSerde("MULTIPOINT (0 0, 0 0)");
    testSerde("MULTIPOINT (0 0, 1 1, 2 3)");
    testSerde("MULTIPOINT EMPTY");
  }

  @Test
  public void testLineString()
      throws Exception {
    testSerde("LINESTRING (0 1, 2 3)");
    testSerde("LINESTRING (0 1, 2 3, 4 5)");
    testSerde("LINESTRING (0 1, 2 3, 4 5, 0 1)");
    testSerde("LINESTRING EMPTY");
  }

  @Test
  public void testMultiLineString()
      throws Exception {
    testSerde("MULTILINESTRING ((0 1, 2 3, 4 5))");
    testSerde("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 5))");
    testSerde("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7), (0 1, 2 3, 4 7, 0 1))");
    testSerde("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7), (0.333 0.74, 0.1 0.2, 2e3 "
        + "4e-3), (0.333 0.74, 2e3 4e-3))");
    testSerde("MULTILINESTRING ((0 1, 2 3, 4 5), (1 1, 2 2))");
    testSerde("MULTILINESTRING EMPTY");
  }

  @Test
  public void testPolygon()
      throws Exception {
    testSerde("POLYGON ((30 10, 40 40, 20 40, 30 10))");
    testSerde("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
    testSerde("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25))");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25), (0.75"
        + " 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25))");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25))");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25), (0.25"
        + " 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25))");
    testSerde("POLYGON EMPTY");
    testSerde("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))");
  }

  @Test
  public void testMultiPolygon()
      throws Exception {
    testSerde("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))");
    testSerde("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((30 20, 45 40, 10 40, 30 20)))");
    testSerde("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 15 5))), ((0 0, 0 1, 1 1, 1 0"
        + ".5, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25))");
    testSerde("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0"
        + ".25 0.75, 0.25 0.25, 0.75 0.25)), ((15 5, 40 10, 10 20, 5 10, 15 5))), ((0 0, 0 1, 1 1, 1 0), (0.25 "
        + "0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25))");
    testSerde("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0"
        + ".75 0.75, 0.75 0.25, 0.25 0.25)))");
    testSerde("MULTIPOLYGON (" + "((30 20, 45 40, 10 40, 30 20)), " +
        // clockwise, counter clockwise
        "((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), " +
        // clockwise, clockwise
        "((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)), " +
        // counter clockwise, clockwise
        "((0 0, 1 0, 1 1, 0 1, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)), " +
        // counter clockwise, counter clockwise
        "((0 0, 1 0, 1 1, 0 1, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), " +
        // counter clockwise, counter clockwise, clockwise
        "((0 0, 1 0, 1 1, 0 1, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25), (0.25 0.25, 0"
        + ".25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)))");
    testSerde("MULTIPOLYGON EMPTY");
  }

  @Test
  public void testGeometryCollection()
      throws Exception {
    testSerde("GEOMETRYCOLLECTION (POINT (1 2))");
    testSerde("GEOMETRYCOLLECTION (POINT (1 2), POINT (2 1), POINT EMPTY)");
    testSerde("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");
    testSerde("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20))))");
    testSerde(
        "GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 " + "5))))");
    testSerde("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 "
        + "5))), POINT (1 2))");
    testSerde("GEOMETRYCOLLECTION (POINT EMPTY)");
    testSerde("GEOMETRYCOLLECTION EMPTY");
    testSerde("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20))), GEOMETRYCOLLECTION "
        + "(MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))))");
  }

  private void testSerde(String wkt)
      throws Exception {
    // test geometry
    testSerde(wkt, GEOMETRY_FACTORY);
    // test geography
    testSerde(wkt, GEOGRAPHY_FACTORY);
  }

  private void testSerde(String wkt, GeometryFactory factory)
      throws Exception {
    Geometry expected = new WKTReader(factory).read(wkt);
    Geometry actual = GeometrySerializer.deserialize(GeometrySerializer.serialize(expected));
    Assert.assertEquals(actual.norm(), expected.norm());
    Assert.assertEquals(actual.getSRID(), factory.getSRID());
  }
}
