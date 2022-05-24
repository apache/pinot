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
package org.apache.pinot.controller.api;

import java.io.IOException;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AccessControlTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  private static final String TABLE_NAME = "accessTestTable";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testAccessDenied() {
    try {
      ControllerTest.sendGetRequest(
          TEST_INSTANCE.getControllerRequestURLBuilder().forSegmentDownload(TABLE_NAME, "testSegment"));
      Assert.fail("Access not denied");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Got error status code: 403 (Forbidden)"));
    }
  }

  public static class DenyAllAccessFactory implements AccessControlFactory {
    private static final AccessControl DENY_ALL_ACCESS = (httpHeaders, tableName) -> !tableName.equals(TABLE_NAME);

    @Override
    public AccessControl create() {
      return DENY_ALL_ACCESS;
    }
  }
}
