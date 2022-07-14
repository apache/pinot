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

import java.util.Collections;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.TableTierInfo;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TableTierResourceTest extends BaseResourceTest {
  @Test
  public void testTableNotFound() {
    Response response = _webTarget.path("tables/unknownTable/tiers").request().get(Response.class);
    assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testSegmentNotFound() {
    String tableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
    Response response =
        _webTarget.path(String.format("segments/%s/unknownSegment/tiers", tableName)).request().get(Response.class);
    assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  public void testTableTierInfo() {
    String tableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    String requestPath = "/tables/" + tableName + "/tiers";
    verifyTableTierInfo(requestPath, tableName, _realtimeIndexSegments.get(0));

    tableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
    requestPath = "/tables/" + tableName + "/tiers";
    verifyTableTierInfo(requestPath, tableName, _offlineIndexSegments.get(0));
  }

  @Test
  public void testTableSegmentTierInfo() {
    String tableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    ImmutableSegment segment = _realtimeIndexSegments.get(0);
    String requestPath = "/segments/" + tableName + "/" + segment.getSegmentName() + "/tiers";
    verifyTableTierInfo(requestPath, tableName, segment);

    tableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
    segment = _offlineIndexSegments.get(0);
    requestPath = "/segments/" + tableName + "/" + segment.getSegmentName() + "/tiers";
    verifyTableTierInfo(requestPath, tableName, segment);
  }

  private void verifyTableTierInfo(String requestPath, String expectedTableName, ImmutableSegment segment) {
    TableTierInfo tableTierInfo = _webTarget.path(requestPath).request().get(TableTierInfo.class);
    assertEquals(tableTierInfo.getTableName(), expectedTableName);
    assertEquals(tableTierInfo.getSegmentTiers().size(), 1);
    assertEquals(tableTierInfo.getSegmentTiers(),
        Collections.singletonMap(segment.getSegmentName(), segment.getTier()));
  }
}
