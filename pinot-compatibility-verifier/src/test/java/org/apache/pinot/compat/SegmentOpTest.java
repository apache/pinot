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
package org.apache.pinot.compat;

import org.apache.pinot.common.restlet.resources.TableView;
import org.apache.pinot.compat.SegmentOp.SegmentStatus;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests segment upload verification against external views. Each invocation uses its own view and no shared state.
public class SegmentOpTest {
  @DataProvider(name = "externalViews")
  public Object[][] externalViews() {
    return new Object[][]{
        {"null", SegmentStatus.PENDING},
        {"{}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":null}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{}}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{\"otherSegment\":{\"server1\":\"ONLINE\"}}}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{\"segment\":null}}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{\"segment\":{}}}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{\"segment\":{\"server1\":\"ONLINE\"}}}", SegmentStatus.ONLINE},
        {"{\"OFFLINE\":{\"segment\":{\"server1\":\"ONLINE\",\"server2\":\"ONLINE\"}}}", SegmentStatus.ONLINE},
        {"{\"OFFLINE\":{\"segment\":{\"server1\":\"ONLINE\",\"server2\":\"OFFLINE\"}}}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{\"segment\":{\"server1\":\"ONLINE\",\"server2\":\"ERROR\"}}}", SegmentStatus.ERROR},
        {"{\"OFFLINE\":{\"segment\":{\"server1\":\"NOT_ONLINE\"}}}", SegmentStatus.PENDING},
        {"{\"OFFLINE\":{\"segment\":{\"server1\":null}}}", SegmentStatus.PENDING}
    };
  }

  @Test(dataProvider = "externalViews")
  public void testSegmentStatus(String response, SegmentStatus expectedStatus)
      throws Exception {
    TableView externalView = JsonUtils.stringToObject(response, TableView.class);
    assertEquals(SegmentOp.getSegmentStatus(externalView, "segment"), expectedStatus);
  }
}
