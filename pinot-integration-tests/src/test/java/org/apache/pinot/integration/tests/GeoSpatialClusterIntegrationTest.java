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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.ByteBuffer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class GeoSpatialClusterIntegrationTest extends BaseClusterIntegrationTest {

  private static final int NUM_TOTAL_DOCS = 1000;
  private static final String DIM_NAME = "dimName";
  private static final String ST_POINT = "st_point";

  private static final String ST_X_NAME = "st_x";
  private static final String ST_Y_NAME = "st_y";
  private static final String WKT_1_NAME = "wkt1";
  private static final String WKT_2_NAME = "wkt2";
  private static final String ST_WITHIN_RESULT_NAME = "st_within_result";

  private static final String AREA_GEOM_NAME = "area_geom";
  private static final String AREA_GEOM_SIZE_NAME = "area_geom_size";
  private static final String AREA_GEOG_NAME = "area_geog";
  private static final String AREA_GEOG_SIZE_NAME = "area_geog_size";

  private static final String[] WKT_1_DATA = new String[]{
      "POINT (25 25)", "POINT (25 25)", "POINT (25 25)", "MULTIPOINT (25 25, 31 31)", "LINESTRING (25 25, 27 27)",
      "MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))", "POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))",
      "POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))", "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "POLYGON EMPTY"
  };

  private static final String[] WKT_2_DATA = new String[]{
      "POINT (20 20)", "MULTIPOINT (20 20, 25 25)", "LINESTRING (20 20, 30 30)", "LINESTRING (20 20, 30 30)",
      "LINESTRING (20 20, 30 30)", "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
      "LINESTRING (20 20, 30 30)", "LINESTRING EMPTY", "LINESTRING (20 20, 30 30)"
  };

  private static final boolean[] ST_WITHIN_RESULT = new boolean[]{
      false, true, true, false, true, false, true, false, true, false, false, false
  };

  private static final String[] AREA_GEOM_DATA = new String[]{
      "POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))", "POLYGON EMPTY", "LINESTRING (1 4, 2 5)", "LINESTRING EMPTY",
      "POINT (1 4)", "POINT EMPTY", "GEOMETRYCOLLECTION EMPTY",
      "GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1)))",
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))",
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), "
          + "GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))))"
  };

  private static final double[] AREA_GEOM_SIZE_DATA = new double[]{
      16.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 6.0, 8.0, 14.0
  };

  private static final String[] AREA_GEOG_DATA = new String[]{
      "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
      "POLYGON((-122.150124 37.486095, -122.149201 37.486606,  -122.145725 37.486580, -122.145923 37.483961, "
          + "-122.149324 37.482480,  -122.150837 37.483238,  -122.150901 37.485392, -122.150124 37.486095))",
      "POLYGON((0 0, 0.008993201943349 0, 0.008993201943349 0.008993201943349, 0 0.008993201943349, 0 0))",
      "POLYGON((90 0, 0 0, 0 90, 90 0))", "POLYGON((90 0, 0 0, 0 90, 90 0), (89 1, 1 1, 1 89, 89 1))",
  };

  private static final double[] AREA_GEOG_SIZE_DATA = new double[]{
      1.2364036567076416E10, 163290.93943479148, 999999.9979474121, 6.375825913974856E13, 3.480423348045961E12
  };

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // create & upload schema AND table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(DIM_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(ST_POINT, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(ST_X_NAME, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(ST_Y_NAME, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(WKT_1_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(WKT_2_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(ST_WITHIN_RESULT_NAME, FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension(AREA_GEOM_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(AREA_GEOM_SIZE_NAME, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(AREA_GEOG_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(AREA_GEOG_SIZE_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    // create & upload segments
    File avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    waitForAllDocsLoaded(60_000);
  }

  @Override
  protected long getCountStarResult() {
    return NUM_TOTAL_DOCS;
  }

  private File createAvroFile()
      throws Exception {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(DIM_NAME, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(ST_X_NAME, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(ST_Y_NAME, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(ST_POINT,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES), null, null),
        new org.apache.avro.Schema.Field(WKT_1_NAME, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(WKT_2_NAME, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(ST_WITHIN_RESULT_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN), null, null),
        new org.apache.avro.Schema.Field(AREA_GEOM_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(AREA_GEOM_SIZE_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(AREA_GEOG_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(AREA_GEOG_SIZE_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null)
    ));

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_TOTAL_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(DIM_NAME, "dim" + i);
        Point point =
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(RANDOM.nextDouble(), RANDOM.nextDouble()));
        record.put(ST_X_NAME, point.getX());
        record.put(ST_Y_NAME, point.getY());
        record.put(ST_POINT, ByteBuffer.wrap(GeometrySerializer.serialize(point)));
        record.put(WKT_1_NAME, WKT_1_DATA[i % WKT_1_DATA.length]);
        record.put(WKT_2_NAME, WKT_2_DATA[i % WKT_2_DATA.length]);
        record.put(ST_WITHIN_RESULT_NAME, ST_WITHIN_RESULT[i % ST_WITHIN_RESULT.length]);
        record.put(AREA_GEOM_NAME, AREA_GEOM_DATA[i % AREA_GEOM_DATA.length]);
        record.put(AREA_GEOM_SIZE_NAME, AREA_GEOM_SIZE_DATA[i % AREA_GEOM_SIZE_DATA.length]);
        record.put(AREA_GEOG_NAME, AREA_GEOG_DATA[i % AREA_GEOG_DATA.length]);
        record.put(AREA_GEOG_SIZE_NAME, AREA_GEOG_SIZE_DATA[i % AREA_GEOG_SIZE_DATA.length]);
        fileWriter.append(record);
      }
    }

    return avroFile;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetHexagonAddress(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "Select geoToH3(20,102,5) from " + DEFAULT_TABLE_NAME;
    JsonNode pinotResponse = postQuery(query);
    long result = pinotResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(result, 599041711439609855L);

    query = "Select geoToH3(-122.419,37.775,6) from " + DEFAULT_TABLE_NAME;
    pinotResponse = postQuery(query);
    result = pinotResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(result, 604189371209351167L);

    query = "Select geoToH3(116.407394,39.904202,6) from " + DEFAULT_TABLE_NAME;
    pinotResponse = postQuery(query);
    result = pinotResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(result, 604356067480043519L);

    query = "Select geoToH3(ST_point(20,102),5) from " + DEFAULT_TABLE_NAME;
    pinotResponse = postQuery(query);
    result = pinotResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(result, 599041711439609855L);

    query = "Select geoToH3(ST_point(-122.419,37.775),6) from " + DEFAULT_TABLE_NAME;
    pinotResponse = postQuery(query);
    result = pinotResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(result, 604189371209351167L);

    query = "Select geoToH3(ST_point(116.407394,39.904202),6) from " + DEFAULT_TABLE_NAME;
    pinotResponse = postQuery(query);
    result = pinotResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(result, 604356067480043519L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStPointLiteralFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    for (int isGeography = 0; isGeography < 2; isGeography++) {
      String query = String.format("Select ST_Point(20, 10, %d) from %s", isGeography, DEFAULT_TABLE_NAME);
      JsonNode pinotResponse = postQuery(query);
      String result = pinotResponse.get("resultTable").get("rows").get(0).get(0).asText();
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(20, 10));
      if (isGeography > 0) {
        GeometryUtils.setGeography(point);
      }
      byte[] expectedValue = GeometrySerializer.serialize(point);
      byte[] actualValue = BytesUtils.toBytes(result);
      assertEquals(actualValue, expectedValue);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStPointFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    for (int isGeography = 0; isGeography < 2; isGeography++) {
      String query =
          String.format("Select ST_Point(st_x, st_y, %d), st_x, st_y from %s", isGeography, DEFAULT_TABLE_NAME);
      JsonNode pinotResponse = postQuery(query);
      JsonNode rows = pinotResponse.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        JsonNode record = rows.get(i);
        String result = record.get(0).asText();
        Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(
            new Coordinate(record.get(1).asDouble(), record.get(2).asDouble()));
        if (isGeography > 0) {
          GeometryUtils.setGeography(point);
        }
        byte[] expectedValue = GeometrySerializer.serialize(point);
        byte[] actualValue = BytesUtils.toBytes(result);
        assertEquals(actualValue, expectedValue);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStWithinQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select ST_Within(ST_GeomFromText(%s), ST_GeomFromText(%s)), %s from %s", WKT_1_NAME, WKT_2_NAME,
            ST_WITHIN_RESULT_NAME, DEFAULT_TABLE_NAME);
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    System.out.println("rows = " + rows);
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      boolean actualResult = row.get(0).intValue() == 1 ? true : false;
      boolean expectedResult = row.get(1).booleanValue();
      Assert.assertEquals(actualResult, expectedResult);
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testStWithinLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    testStWithinResult("POINT (25 25)", "POINT (20 20)", false);
    testStWithinResult("POINT (25 25)", "MULTIPOINT (20 20, 25 25)", true);
    testStWithinResult("POINT (25 25)", "LINESTRING (20 20, 30 30)", true);
    testStWithinResult("MULTIPOINT (25 25, 31 31)", "LINESTRING (20 20, 30 30)", false);
    testStWithinResult("LINESTRING (25 25, 27 27)", "LINESTRING (20 20, 30 30)", true);
    testStWithinResult("MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))",
        "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", false);
    testStWithinResult("POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", true);
    testStWithinResult("POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
        false);
    testStWithinResult("POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
        "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))", true);
    testStWithinResult("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "LINESTRING (20 20, 30 30)", false);
    testStWithinResult("POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "LINESTRING EMPTY", false);
    testStWithinResult("POLYGON EMPTY", "LINESTRING (20 20, 30 30)", false);
  }

  private void testStWithinResult(String leftWkt, String rightWkt, boolean result)
      throws Exception {
    String queryFormat = "Select ST_Within(ST_GeomFromText('%s'), ST_GeomFromText('%s')) from " + DEFAULT_TABLE_NAME;
    String query = String.format(queryFormat, leftWkt, rightWkt);
    JsonNode pinotResponse = postQuery(query);
    int actualResult = pinotResponse.get("resultTable").get("rows").get(0).get(0).intValue();
    Assert.assertEquals(actualResult, result ? 1 : 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStAreaQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select ST_Area(ST_GeomFromText(%s)), %s, ST_Area(ST_GeogFromText(%s)), %s from %s",
            AREA_GEOM_NAME, AREA_GEOM_SIZE_NAME, AREA_GEOG_NAME, AREA_GEOG_SIZE_NAME, DEFAULT_TABLE_NAME);
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      int actualResult = row.get(0).intValue();
      int expectedResult = row.get(1).intValue();
      Assert.assertEquals(actualResult, expectedResult);
      actualResult = row.get(2).intValue();
      expectedResult = row.get(3).intValue();
      Assert.assertEquals(actualResult, expectedResult);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStUnionQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select STUnion(ST_GeogFromText(%s)) from %s",
            AREA_GEOG_NAME, DEFAULT_TABLE_NAME);
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    String actualResult = rows.get(0).get(0).asText();
    String expectedResult =
        "850000000200000012000000000000000a0000000000000000000000000000000000000000000000003f826b0721dd331700000"
            + "000000000003ff0000000000000000000000000000040568000000000003ff00000000000004056400000000000405640000000"
            + "00003ff0000000000000405680000000000000000000000000003ff000000000000000000000000000003f826b0721dd3317000"
            + "000000000000000000000000000000000000000000000c05e899ba1b196104042be385c67dfe3c05e898c8259e1f44042be491a"
            + "fc04c9c05e89538ef34d6a4042be4840e1719fc05e8956cd6c2efd4042bdf26f1dc50dc05e898e864020814042bdc1e7967cafc"
            + "05e89a7503b81b64042bddabe27179cc05e89a85caafbc24042be215336deb9c05e899ba1b196104042be385c67dfe3";
    Assert.assertEquals(actualResult, expectedResult);
  }
}
