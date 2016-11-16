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
import java.util.List;
import javax.ws.rs.core.Response;
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
}
