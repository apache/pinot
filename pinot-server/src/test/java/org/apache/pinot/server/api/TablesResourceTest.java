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
package org.apache.pinot.server.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.TablesList;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TablesResourceTest extends BaseResourceTest {

  @Test
  public void getTables()
      throws Exception {
    String tablesPath = "/tables";

    Response response = _webTarget.path(tablesPath).request().get(Response.class);
    String responseBody = response.readEntity(String.class);
    TablesList tablesList = JsonUtils.stringToObject(responseBody, TablesList.class);

    Assert.assertNotNull(tablesList);
    List<String> tables = tablesList.getTables();
    Assert.assertNotNull(tables);
    Assert.assertEquals(tables.size(), 1);
    Assert.assertEquals(tables.get(0), TABLE_NAME);

    String secondTable = "secondTable";
    addTable(secondTable);
    response = _webTarget.path(tablesPath).request().get(Response.class);
    responseBody = response.readEntity(String.class);
    tablesList = JsonUtils.stringToObject(responseBody, TablesList.class);

    Assert.assertNotNull(tablesList);
    tables = tablesList.getTables();
    Assert.assertNotNull(tables);
    Assert.assertEquals(tables.size(), 2);
    Assert.assertTrue(tables.contains(TABLE_NAME));
    Assert.assertTrue(tables.contains(secondTable));
  }

  @Test
  public void getSegments()
      throws Exception {
    String segmentsPath = "/tables/" + TABLE_NAME + "/segments";
    IndexSegment defaultSegment = _indexSegments.get(0);

    TableSegments tableSegments = _webTarget.path(segmentsPath).request().get(TableSegments.class);
    Assert.assertNotNull(tableSegments);
    List<String> segmentNames = tableSegments.getSegments();
    Assert.assertNotNull(segmentNames);
    Assert.assertEquals(segmentNames.size(), 1);
    Assert.assertEquals(segmentNames.get(0), _indexSegments.get(0).getSegmentName());

    IndexSegment secondSegment = setUpSegment("0");
    tableSegments = _webTarget.path(segmentsPath).request().get(TableSegments.class);
    Assert.assertNotNull(tableSegments);
    segmentNames = tableSegments.getSegments();
    Assert.assertNotNull(segmentNames);
    Assert.assertEquals(segmentNames.size(), 2);
    Assert.assertTrue(segmentNames.contains(defaultSegment.getSegmentName()));
    Assert.assertTrue(segmentNames.contains(secondSegment.getSegmentName()));

    // No such table
    Response response = _webTarget.path("/tables/noSuchTable/segments").request().get(Response.class);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testSegmentMetadata()
      throws Exception {
    IndexSegment defaultSegment = _indexSegments.get(0);
    String segmentMetadataPath = "/tables/" + TABLE_NAME + "/segments/" + defaultSegment.getSegmentName() + "/metadata";

    JsonNode jsonResponse =
        JsonUtils.stringToJsonNode(_webTarget.path(segmentMetadataPath).request().get(String.class));
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) defaultSegment.getSegmentMetadata();
    Assert.assertEquals(jsonResponse.get("segmentName").asText(), segmentMetadata.getName());
    Assert.assertEquals(jsonResponse.get("crc").asText(), segmentMetadata.getCrc());
    Assert.assertEquals(jsonResponse.get("creationTimeMillis").asLong(), segmentMetadata.getIndexCreationTime());
    Assert.assertEquals(jsonResponse.get("paddingCharacter").asText(),
        String.valueOf(segmentMetadata.getPaddingCharacter()));
    Assert.assertEquals(jsonResponse.get("refreshTimeMillis").asLong(), segmentMetadata.getRefreshTime());
    Assert.assertEquals(jsonResponse.get("pushTimeMillis").asLong(), segmentMetadata.getPushTime());
    Assert.assertTrue(jsonResponse.has("pushTimeReadable"));
    Assert.assertTrue(jsonResponse.has("refreshTimeReadable"));
    Assert.assertTrue(jsonResponse.has("startTimeReadable"));
    Assert.assertTrue(jsonResponse.has("endTimeReadable"));
    Assert.assertTrue(jsonResponse.has("creationTimeReadable"));
    Assert.assertEquals(jsonResponse.get("columns").size(), 0);

    jsonResponse = JsonUtils.stringToJsonNode(
        _webTarget.path(segmentMetadataPath).queryParam("columns", "column1").queryParam("columns", "column2").request()
            .get(String.class));
    Assert.assertEquals(jsonResponse.get("columns").size(), 2);

    jsonResponse = JsonUtils.stringToJsonNode(
        (_webTarget.path(segmentMetadataPath).queryParam("columns", "*").request().get(String.class)));
    Assert.assertEquals(jsonResponse.get("columns").size(), segmentMetadata.getAllColumns().size());

    Response response = _webTarget.path("/tables/UNKNOWN_TABLE/segments/" + defaultSegment.getSegmentName()).request()
        .get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

    response = _webTarget.path("/tables/" + TABLE_NAME + "/segments/UNKNOWN_SEGMENT").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testSegmentCrcMetadata()
      throws Exception {
    String segmentsCrcPath = "/tables/" + TABLE_NAME + "/segments/crc";

    // Upload segments
    List<ImmutableSegment> immutableSegments = setUpSegments(2);

    // Trigger crc api to fetch crc information
    String response = _webTarget.path(segmentsCrcPath).request().get(String.class);
    JsonNode segmentsCrc = JsonUtils.stringToJsonNode(response);

    // Check that crc info is correct
    for (ImmutableSegment immutableSegment : immutableSegments) {
      String segmentName = immutableSegment.getSegmentName();
      String crc = immutableSegment.getSegmentMetadata().getCrc();
      Assert.assertEquals(segmentsCrc.get(segmentName).asText(), crc);
    }
  }
}
