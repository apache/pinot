/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.server.api.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.restlet.resources.TableSegments;
import com.linkedin.pinot.common.restlet.resources.TablesList;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import com.fasterxml.jackson.core.type.TypeReference;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;


public class TablesResourceTest {

  ResourceTestHelper testHelper = new ResourceTestHelper();

  @BeforeClass
  public void setupTest()
      throws Exception {
    testHelper.setup();
  }

  @AfterClass
  public void teardownTest()
      throws Exception {
    testHelper.tearDown();
  }

  @Test
  public void getTables()
      throws Exception {
    Response response =
        testHelper.target.path("/tables").request().get(Response.class);
    String responseBody = response.readEntity(String.class);
    TablesList tablesList =
        new ObjectMapper().readValue(responseBody, TablesList.class);
    assertNotNull(tablesList);
    List<String> tables = tablesList.getTables();
    assertNotNull(tables);
    assertEquals(tables.size(), 1);
    assertEquals(tables.get(0), ResourceTestHelper.DEFAULT_TABLE_NAME);

    final String secondTable = "secondTable";
    testHelper.addTable(secondTable);
    IndexSegment secondSegment = testHelper.setupSegment(secondTable, ResourceTestHelper.DEFAULT_AVRO_DATA_FILE, "2");
    tablesList = testHelper.target.path("/tables").request().get(TablesList.class);
    assertNotNull(tablesList);
    assertNotNull(tablesList.getTables());
    assertEquals(tablesList.getTables().size(), 2);
    assertTrue(tablesList.getTables().contains(ResourceTestHelper.DEFAULT_TABLE_NAME));
    assertTrue(tablesList.getTables().contains(secondTable));
  }

  @Test
  public void getSegments()
      throws Exception {
    {
      TableSegments tableSegments =
          testHelper.target.path("/tables/" + ResourceTestHelper.DEFAULT_TABLE_NAME + "/segments").request()
              .get(TableSegments.class);
      assertNotNull(tableSegments);
      assertNotNull(tableSegments.getSegments());
      assertEquals(tableSegments.getSegments().size(), 1);
      assertEquals(tableSegments.getSegments().get(0), testHelper.indexSegment.getSegmentName());

      IndexSegment secondSegment = testHelper
          .setupSegment(ResourceTestHelper.DEFAULT_TABLE_NAME, ResourceTestHelper.DEFAULT_AVRO_DATA_FILE, "2");
      tableSegments = testHelper.target.path("/tables/" + ResourceTestHelper.DEFAULT_TABLE_NAME + "/segments").request()
          .get(TableSegments.class);
      assertNotNull(tableSegments);
      assertNotNull(tableSegments.getSegments());
      assertEquals(tableSegments.getSegments().size(), 2);
      assertTrue(tableSegments.getSegments().contains(testHelper.indexSegment.getSegmentName()));
      assertTrue(tableSegments.getSegments().contains(secondSegment.getSegmentName()));
    }
    {
      // No such table
      Response response = testHelper.target.path("/tables/noSuchTable/segments").request().get(Response.class);
      assertNotNull(response);
      assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testSegmentMetadata()
      throws JSONException {
    final String urlFormat = "/tables/%s/segments/%s/metadata";
    {
      String response = testHelper.target.path(String.format(urlFormat, ResourceTestHelper.DEFAULT_TABLE_NAME,
          testHelper.indexSegment.getSegmentMetadata().getName())).request().get(String.class);
      JSONObject jsonMeta = new JSONObject(response);
      SegmentMetadataImpl segmentMeta = (SegmentMetadataImpl) testHelper.indexSegment.getSegmentMetadata();
      assertEquals(jsonMeta.getString("segmentName"), segmentMeta.getName());
      assertEquals(jsonMeta.getString("crc"), segmentMeta.getCrc());
      assertEquals(jsonMeta.getLong("creationTimeMillis"), segmentMeta.getIndexCreationTime());
      assertEquals(jsonMeta.getString("paddingCharacter"), String.valueOf(segmentMeta.getPaddingCharacter()));
      assertEquals(jsonMeta.getLong("refreshTimeMillis"), segmentMeta.getRefreshTime());
      assertEquals(jsonMeta.getLong("pushTimeMillis"), segmentMeta.getPushTime());
      assertTrue(jsonMeta.has("pushTimeReadable"));
      assertTrue(jsonMeta.has("refreshTimeReadable"));
      assertTrue(jsonMeta.has("startTimeReadable"));
      assertTrue(jsonMeta.has("endTimeReadable"));
      assertTrue(jsonMeta.has("creationTimeReadable"));
      assertEquals(jsonMeta.getLong("startTimeMillis"), segmentMeta.getTimeInterval().getStartMillis());
      assertEquals(jsonMeta.getLong("endTimeMillis"), segmentMeta.getTimeInterval().getEndMillis());

      JSONArray columns = jsonMeta.getJSONArray("columns");
      assertEquals(columns.length(), 0);
    }
    {
      Response response = testHelper.target.path(String.format(urlFormat, ResourceTestHelper.DEFAULT_TABLE_NAME,
          testHelper.indexSegment.getSegmentMetadata().getName()))
          .queryParam("columns", "column1")
          .queryParam("columns", "column2")
          .request().get();
      System.out.println(response.getStatus());
      JSONObject jsonMeta = new JSONObject(response.readEntity(String.class));
      JSONArray columns = jsonMeta.getJSONArray("columns");
      assertEquals(columns.length(), 2);
    }
    {
      String response = testHelper.target.path(String.format(urlFormat, ResourceTestHelper.DEFAULT_TABLE_NAME,
          testHelper.indexSegment.getSegmentMetadata().getName()))
          .queryParam("columns", "*")
          .request().get(String.class);
      JSONObject jsonMeta = new JSONObject(response);
      JSONArray columns = jsonMeta.getJSONArray("columns");
      assertEquals(columns.length(),
          ((SegmentMetadataImpl) testHelper.indexSegment.getSegmentMetadata()).getAllColumns().size());
    }
    {
      Response response = testHelper.target.path(String
          .format(urlFormat, "UNKNOWN_TABLE", testHelper.indexSegment.getSegmentMetadata().getName()))
          .request().get(Response.class);
      assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }
    {
      Response response = testHelper.target
          .path(String.format(urlFormat, ResourceTestHelper.DEFAULT_TABLE_NAME, "UNKNOWN_SEGMENT"))
          .request().get(Response.class);
      assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testSegmentCrcMetadata() throws Exception {
    final String urlFormat = "/tables/%s/segments/crc";

    // Upload 10 segments
    List<IndexSegment> segments = testHelper.setUpSegments(10);

    // Trigger crc api to fetch crc information
    String response = testHelper.target.path(String.format(urlFormat, ResourceTestHelper.DEFAULT_TABLE_NAME))
        .request().get(String.class);
    JSONObject jsonMeta = new JSONObject(response);
    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> crcMap = mapper.readValue(response, new TypeReference<Map<String, Object>>(){});

    // Check that crc info is correct
    for(IndexSegment segment : segments) {
      String segmentName = segment.getSegmentName();
      String crc = segment.getSegmentMetadata().getCrc();
      assertEquals(crcMap.get(segmentName), crc);
    }
  }
}
