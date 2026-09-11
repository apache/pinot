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
package org.apache.pinot.controller.api.resources;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Exercises the table-config HTTP authorization boundary with a principal that has table READ permission only.
public class PinotReadOnlyTableConfigResourceTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "readOnlyConfig";
  private static final String LITERAL_PASSWORD = "read-only-literal-password";
  private static final String PASSWORD_PLACEHOLDER = "${READ_ONLY_PASSWORD}";
  private static final Map<String, String> ADMIN_HEADERS =
      Map.of("Authorization", "Basic YWRtaW46dmVyeXNlY3JldA==");
  private static final Map<String, String> READ_ONLY_HEADERS =
      Map.of("Authorization", "Basic dXNlcjpzZWNyZXQ=");
  private static final Map<String, String> RAW_READER_HEADERS =
      Map.of("Authorization", "Basic cmF3OnJhd3NlY3JldA==");

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> configuration = getDefaultControllerConfiguration();
    configuration.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    configuration.put("controller.admin.access.control.principals", "admin,user,raw");
    configuration.put("controller.admin.access.control.principals.admin.password", "verysecret");
    configuration.put("controller.admin.access.control.principals.admin.permissions",
        "create,read,update,delete,GetZnode");
    configuration.put("controller.admin.access.control.principals.user.password", "secret");
    configuration.put("controller.admin.access.control.principals.user.permissions", "read");
    configuration.put("controller.admin.access.control.principals.raw.password", "rawsecret");
    configuration.put("controller.admin.access.control.principals.raw.permissions", "GetZnode");
    startController(configuration);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    addDummySchema(RAW_TABLE_NAME);
    TableConfig tableConfig = createDummyTableConfig(RAW_TABLE_NAME, TableType.OFFLINE);
    addTableConfig(tableConfig);

    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put("provider.password", LITERAL_PASSWORD);
    customConfigs.put("placeholder.password", PASSWORD_PLACEHOLDER);
    customConfigs.put("client.id", "preserved-client");
    tableConfig.setCustomConfig(new TableCustomConfig(customConfigs));
    // Keep the placeholder unresolved in the stored representation. The ordinary create path performs runtime
    // validation that can resolve environment-provider values, which would make this output-boundary test depend on
    // the test process environment.
    assertTrue(ZKMetadataProvider.setTableConfig(_propertyStore, tableConfig));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    cleanup();
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @Override
  protected Map<String, String> getAdminClientHeaders() {
    return ADMIN_HEADERS;
  }

  @Test
  public void testReadOnlyPrincipalReceivesOnlyRedactedConfigs()
      throws Exception {
    String tableResponse = sendGetRequest(
        controllerUrl("/tables/" + RAW_TABLE_NAME + "?type=OFFLINE"), READ_ONLY_HEADERS);
    String combinedResponse = sendGetRequest(
        controllerUrl("/tableConfigs/" + RAW_TABLE_NAME), READ_ONLY_HEADERS);

    for (String response : new String[]{tableResponse, combinedResponse}) {
      assertFalse(response.contains(LITERAL_PASSWORD), response);
      assertTrue(response.contains(TableConfigRedactionUtils.REDACTION_MARKER), response);
      assertTrue(response.contains(PASSWORD_PLACEHOLDER), response);
      assertTrue(response.contains("preserved-client"), response);
    }
  }

  @Test
  public void testRawTableConfigZkReadRequiresGetZnodePermission()
      throws Exception {
    String tableConfigPath = "/" + getHelixClusterName() + "/PROPERTYSTORE"
        + ZKMetadataProvider.constructPropertyStorePathForResourceConfig(RAW_TABLE_NAME + "_OFFLINE");
    String rawTableConfigUrl = controllerUrl("/zk/get?path=" + tableConfigPath);

    Pair<Integer, String> readOnlyResponse = sendGetRequestWithStatusCode(rawTableConfigUrl, READ_ONLY_HEADERS);
    assertEquals(readOnlyResponse.getLeft().intValue(), Response.Status.FORBIDDEN.getStatusCode());

    Pair<Integer, String> rawReaderResponse = sendGetRequestWithStatusCode(rawTableConfigUrl, RAW_READER_HEADERS);
    assertEquals(rawReaderResponse.getLeft().intValue(), Response.Status.OK.getStatusCode());
    assertTrue(rawReaderResponse.getRight().contains(LITERAL_PASSWORD),
        "A principal with getznode permission should receive the stored ZK representation");

    Pair<Integer, String> adminResponse = sendGetRequestWithStatusCode(rawTableConfigUrl, ADMIN_HEADERS);
    assertEquals(adminResponse.getLeft().intValue(), Response.Status.OK.getStatusCode());
  }
}
