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
import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Response;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
      throws IOException {
    Response response =
        testHelper.target.path("/tables").request().get(Response.class);
    String responseBody = response.readEntity(String.class);
    TablesList tablesList =
        new ObjectMapper().readValue(responseBody, TablesList.class);
    Assert.assertNotNull(tablesList);
    List<String> tables = tablesList.getTables();
    Assert.assertNotNull(tables);
    Assert.assertEquals(tables.size(), 1);
    Assert.assertEquals(tables.get(0), ResourceTestHelper.DEFAULT_TABLE_NAME);
  }

  @Test
  public void getSegments() {
    {
      TableSegments tableSegments =
          testHelper.target.path("/tables/" + ResourceTestHelper.DEFAULT_TABLE_NAME + "/segments").request()
              .get(TableSegments.class);
      Assert.assertNotNull(tableSegments);
      Assert.assertNotNull(tableSegments.getSegments());
      Assert.assertEquals(tableSegments.getSegments().size(), 1);
      Assert.assertEquals(tableSegments.getSegments().get(0), testHelper.indexSegment.getSegmentName());
    }
    {
      // No such table
      Response response = testHelper.target.path("/tables/noSuchTable/segments").request().get(Response.class);
      Assert.assertNotNull(response);
      Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }
  }
}
