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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.UserConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link AccessControlUserConfigUtils}
 */
public class AccessControlUserConfigUtilsTest {

  private static final String TEST_USERNAME = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  @Test
  public void testFromZNRecordWithMandatoryFieldsOnly() {
    // Create ZNRecord with only mandatory fields
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, TEST_USERNAME);
    simpleFields.put(UserConfig.PASSWORD_KEY, TEST_PASSWORD);
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.BROKER.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.USER.toString());
    znRecord.setSimpleFields(simpleFields);

    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig, "UserConfig should not be null");
    Assert.assertEquals(userConfig.getUserName(), TEST_USERNAME, "Username should match");
    Assert.assertEquals(userConfig.getPassword(), TEST_PASSWORD, "Password should match");
    Assert.assertEquals(userConfig.getComponentType(), ComponentType.BROKER, "Component type should match");
    Assert.assertEquals(userConfig.getRoleType(), RoleType.USER, "Role type should match");
    Assert.assertNull(userConfig.getTables(), "Tables should be null");
    Assert.assertNull(userConfig.getExcludeTables(), "Exclude tables should be null");
    Assert.assertNull(userConfig.getPermissios(), "Permissions should be null");
    Assert.assertNull(userConfig.getRlsFilters(), "RLS filters should be null");
  }

  @Test
  public void testFromZNRecordWithTableList() {
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, TEST_USERNAME);
    simpleFields.put(UserConfig.PASSWORD_KEY, TEST_PASSWORD);
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.CONTROLLER.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.ADMIN.toString());
    znRecord.setSimpleFields(simpleFields);

    List<String> tableList = Arrays.asList("table1", "table2", "table3");
    znRecord.setListField(UserConfig.TABLES_KEY, tableList);

    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig.getTables(), "Tables should not be null");
    Assert.assertEquals(userConfig.getTables(), tableList, "Table list should match");
    Assert.assertEquals(userConfig.getTables().size(), 3, "Table list size should be 3");
  }

  @Test
  public void testFromZNRecordWithExcludeTableList() {
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, TEST_USERNAME);
    simpleFields.put(UserConfig.PASSWORD_KEY, TEST_PASSWORD);
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.SERVER.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.USER.toString());
    znRecord.setSimpleFields(simpleFields);

    List<String> excludeTableList = Arrays.asList("excludeTable1", "excludeTable2");
    znRecord.setListField(UserConfig.EXCLUDE_TABLES_KEY, excludeTableList);

    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig.getExcludeTables(), "Exclude tables should not be null");
    Assert.assertEquals(userConfig.getExcludeTables(), excludeTableList, "Exclude table list should match");
    Assert.assertEquals(userConfig.getExcludeTables().size(), 2, "Exclude table list size should be 2");
  }

  @Test
  public void testFromZNRecordWithPermissionList() {
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, TEST_USERNAME);
    simpleFields.put(UserConfig.PASSWORD_KEY, TEST_PASSWORD);
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.MINION.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.ADMIN.toString());
    znRecord.setSimpleFields(simpleFields);

    List<String> permissionListStr = Arrays.asList("READ", "CREATE", "UPDATE");
    znRecord.setListField(UserConfig.PERMISSIONS_KEY, permissionListStr);

    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig.getPermissios(), "Permissions should not be null");
    Assert.assertEquals(userConfig.getPermissios().size(), 3, "Permission list size should be 3");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.READ), "Should contain READ permission");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.CREATE), "Should contain CREATE permission");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.UPDATE), "Should contain UPDATE permission");
  }

  @Test
  public void testFromZNRecordWithRlsFilters() throws JsonProcessingException {
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, TEST_USERNAME);
    simpleFields.put(UserConfig.PASSWORD_KEY, TEST_PASSWORD);
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.BROKER.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.USER.toString());

    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("country='US'", "department='Engineering'"));
    rlsFilters.put("table2", Arrays.asList("region='WEST'"));
    simpleFields.put(UserConfig.RLS_FILTERS_KEY, JsonUtils.objectToString(rlsFilters));

    znRecord.setSimpleFields(simpleFields);

    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig.getRlsFilters(), "RLS filters should not be null");
    Assert.assertEquals(userConfig.getRlsFilters().size(), 2, "RLS filters map size should be 2");
    Assert.assertTrue(userConfig.getRlsFilters().containsKey("table1"), "Should contain table1 filters");
    Assert.assertTrue(userConfig.getRlsFilters().containsKey("table2"), "Should contain table2 filters");
    Assert.assertEquals(userConfig.getRlsFilters().get("table1").size(), 2, "table1 should have 2 filters");
    Assert.assertEquals(userConfig.getRlsFilters().get("table2").size(), 1, "table2 should have 1 filter");
    // Verify actual filter values
    Assert.assertEquals(userConfig.getRlsFilters().get("table1").get(0), "country='US'");
    Assert.assertEquals(userConfig.getRlsFilters().get("table1").get(1), "department='Engineering'");
    Assert.assertEquals(userConfig.getRlsFilters().get("table2").get(0), "region='WEST'");
  }

  @Test
  public void testFromZNRecordWithMalformedRlsFilters() {
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, TEST_USERNAME);
    simpleFields.put(UserConfig.PASSWORD_KEY, TEST_PASSWORD);
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.BROKER.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.USER.toString());
    simpleFields.put(UserConfig.RLS_FILTERS_KEY, "invalid-json-{malformed");
    znRecord.setSimpleFields(simpleFields);

    // Should not throw exception, but log error and continue
    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig, "UserConfig should not be null");
    Assert.assertNull(userConfig.getRlsFilters(), "RLS filters should be null due to malformed JSON");
  }

  @Test
  public void testFromZNRecordWithAllFields() throws JsonProcessingException {
    ZNRecord znRecord = new ZNRecord(TEST_USERNAME);
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(UserConfig.USERNAME_KEY, "adminUser");
    simpleFields.put(UserConfig.PASSWORD_KEY, "adminPass123");
    simpleFields.put(UserConfig.COMPONET_KEY, ComponentType.PROXY.toString());
    simpleFields.put(UserConfig.ROLE_KEY, RoleType.ADMIN.toString());

    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("filter1", "filter2"));
    simpleFields.put(UserConfig.RLS_FILTERS_KEY, JsonUtils.objectToString(rlsFilters));
    znRecord.setSimpleFields(simpleFields);

    List<String> tableList = Arrays.asList("table1", "table2");
    List<String> excludeTableList = Arrays.asList("excludeTable1");
    List<String> permissionListStr = Arrays.asList("READ", "UPDATE", "DELETE");

    znRecord.setListField(UserConfig.TABLES_KEY, tableList);
    znRecord.setListField(UserConfig.EXCLUDE_TABLES_KEY, excludeTableList);
    znRecord.setListField(UserConfig.PERMISSIONS_KEY, permissionListStr);

    UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertNotNull(userConfig, "UserConfig should not be null");
    Assert.assertEquals(userConfig.getUserName(), "adminUser", "Username should match");
    Assert.assertEquals(userConfig.getPassword(), "adminPass123", "Password should match");
    Assert.assertEquals(userConfig.getComponentType(), ComponentType.PROXY, "Component type should match");
    Assert.assertEquals(userConfig.getRoleType(), RoleType.ADMIN, "Role type should match");
    Assert.assertEquals(userConfig.getTables(), tableList, "Table list should match");
    Assert.assertEquals(userConfig.getExcludeTables(), excludeTableList, "Exclude table list should match");
    Assert.assertEquals(userConfig.getPermissios().size(), 3, "Permission list size should match");
    Assert.assertNotNull(userConfig.getRlsFilters(), "RLS filters should not be null");
  }

  @Test
  public void testToZNRecordWithMandatoryFieldsOnly() throws JsonProcessingException {
    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    Assert.assertNotNull(znRecord, "ZNRecord should not be null");
    Assert.assertEquals(znRecord.getId(), TEST_USERNAME, "ZNRecord ID should match username");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.USERNAME_KEY), TEST_USERNAME, "Username should match");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.PASSWORD_KEY), TEST_PASSWORD, "Password should match");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.COMPONET_KEY), ComponentType.BROKER.toString(),
        "Component type should match");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.ROLE_KEY), RoleType.USER.toString(),
        "Role type should match");
    Assert.assertNull(znRecord.getListField(UserConfig.TABLES_KEY), "Tables should be null");
    Assert.assertNull(znRecord.getListField(UserConfig.EXCLUDE_TABLES_KEY), "Exclude tables should be null");
    Assert.assertNull(znRecord.getListField(UserConfig.PERMISSIONS_KEY), "Permissions should be null");
    Assert.assertNull(znRecord.getSimpleField(UserConfig.RLS_FILTERS_KEY), "RLS filters should be null");
  }

  @Test
  public void testToZNRecordWithTableList() throws JsonProcessingException {
    List<String> tableList = Arrays.asList("table1", "table2", "table3");
    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.CONTROLLER)
        .setRoleType(RoleType.ADMIN)
        .setTableList(tableList)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    Assert.assertNotNull(znRecord.getListField(UserConfig.TABLES_KEY), "Tables should not be null");
    Assert.assertEquals(znRecord.getListField(UserConfig.TABLES_KEY), tableList, "Table list should match");
    Assert.assertEquals(znRecord.getListField(UserConfig.TABLES_KEY).size(), 3, "Table list size should be 3");
  }

  @Test
  public void testToZNRecordWithExcludeTableList() throws JsonProcessingException {
    List<String> excludeTableList = Arrays.asList("excludeTable1", "excludeTable2");
    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.SERVER)
        .setRoleType(RoleType.USER)
        .setExcludeTableList(excludeTableList)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    Assert.assertNotNull(znRecord.getListField(UserConfig.EXCLUDE_TABLES_KEY), "Exclude tables should not be null");
    Assert.assertEquals(znRecord.getListField(UserConfig.EXCLUDE_TABLES_KEY), excludeTableList,
        "Exclude table list should match");
    Assert.assertEquals(znRecord.getListField(UserConfig.EXCLUDE_TABLES_KEY).size(), 2,
        "Exclude table list size should be 2");
  }

  @Test
  public void testToZNRecordWithPermissionList() throws JsonProcessingException {
    List<AccessType> permissionList = Arrays.asList(AccessType.READ, AccessType.CREATE, AccessType.UPDATE);
    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.MINION)
        .setRoleType(RoleType.ADMIN)
        .setPermissionList(permissionList)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    List<String> permissionListStr = znRecord.getListField(UserConfig.PERMISSIONS_KEY);
    Assert.assertNotNull(permissionListStr, "Permissions should not be null");
    Assert.assertEquals(permissionListStr.size(), 3, "Permission list size should be 3");
    Assert.assertTrue(permissionListStr.contains("READ"), "Should contain READ permission");
    Assert.assertTrue(permissionListStr.contains("CREATE"), "Should contain CREATE permission");
    Assert.assertTrue(permissionListStr.contains("UPDATE"), "Should contain UPDATE permission");
  }

  @Test
  public void testToZNRecordWithRlsFilters() throws IOException {
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("country='US'", "department='Engineering'"));
    rlsFilters.put("table2", Arrays.asList("region='WEST'"));

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .setRlsFilters(rlsFilters)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    String rlsFiltersJson = znRecord.getSimpleField(UserConfig.RLS_FILTERS_KEY);
    Assert.assertNotNull(rlsFiltersJson, "RLS filters should not be null");

    // Deserialize and verify
    Map<String, List<String>> deserializedFilters = JsonUtils.stringToObject(
        rlsFiltersJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, List<String>>>() {
        }
    );
    Assert.assertEquals(deserializedFilters.size(), 2, "RLS filters map size should be 2");
    Assert.assertTrue(deserializedFilters.containsKey("table1"), "Should contain table1 filters");
    Assert.assertTrue(deserializedFilters.containsKey("table2"), "Should contain table2 filters");
    // Verify actual filter values
    Assert.assertEquals(deserializedFilters.get("table1").get(0), "country='US'");
    Assert.assertEquals(deserializedFilters.get("table1").get(1), "department='Engineering'");
    Assert.assertEquals(deserializedFilters.get("table2").get(0), "region='WEST'");
  }

  @Test
  public void testToZNRecordWithAllFields() throws JsonProcessingException {
    List<String> tableList = Arrays.asList("table1", "table2");
    List<String> excludeTableList = Arrays.asList("excludeTable1");
    List<AccessType> permissionList = Arrays.asList(AccessType.READ, AccessType.UPDATE, AccessType.DELETE);
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("filter1", "filter2"));

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername("adminUser")
        .setPassword("adminPass123")
        .setComponentType(ComponentType.PROXY)
        .setRoleType(RoleType.ADMIN)
        .setTableList(tableList)
        .setExcludeTableList(excludeTableList)
        .setPermissionList(permissionList)
        .setRlsFilters(rlsFilters)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    Assert.assertNotNull(znRecord, "ZNRecord should not be null");
    Assert.assertEquals(znRecord.getId(), "adminUser", "ZNRecord ID should match username");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.USERNAME_KEY), "adminUser", "Username should match");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.PASSWORD_KEY), "adminPass123", "Password should match");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.COMPONET_KEY), ComponentType.PROXY.toString(),
        "Component type should match");
    Assert.assertEquals(znRecord.getSimpleField(UserConfig.ROLE_KEY), RoleType.ADMIN.toString(),
        "Role type should match");
    Assert.assertEquals(znRecord.getListField(UserConfig.TABLES_KEY), tableList, "Table list should match");
    Assert.assertEquals(znRecord.getListField(UserConfig.EXCLUDE_TABLES_KEY), excludeTableList,
        "Exclude table list should match");
    Assert.assertNotNull(znRecord.getListField(UserConfig.PERMISSIONS_KEY), "Permissions should not be null");
    Assert.assertNotNull(znRecord.getSimpleField(UserConfig.RLS_FILTERS_KEY), "RLS filters should not be null");
  }

  @Test
  public void testRoundTripConversionWithMandatoryFields() throws JsonProcessingException {
    UserConfig originalConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(originalConfig);
    UserConfig reconstructedConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertEquals(reconstructedConfig.getUserName(), originalConfig.getUserName(),
        "Username should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getPassword(), originalConfig.getPassword(),
        "Password should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getComponentType(), originalConfig.getComponentType(),
        "Component type should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getRoleType(), originalConfig.getRoleType(),
        "Role type should match after round-trip");
  }

  @Test
  public void testRoundTripConversionWithAllFields() throws JsonProcessingException {
    List<String> tableList = Arrays.asList("table1", "table2");
    List<String> excludeTableList = Arrays.asList("excludeTable1");
    List<AccessType> permissionList = Arrays.asList(AccessType.CREATE, AccessType.READ, AccessType.UPDATE,
        AccessType.DELETE);
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("country='US'", "department='Engineering'"));
    rlsFilters.put("table2", Arrays.asList("region='WEST'"));

    UserConfig originalConfig = new UserConfigBuilder()
        .setUsername("fullTestUser")
        .setPassword("fullTestPassword")
        .setComponentType(ComponentType.CONTROLLER)
        .setRoleType(RoleType.ADMIN)
        .setTableList(tableList)
        .setExcludeTableList(excludeTableList)
        .setPermissionList(permissionList)
        .setRlsFilters(rlsFilters)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(originalConfig);
    UserConfig reconstructedConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertEquals(reconstructedConfig.getUserName(), originalConfig.getUserName(),
        "Username should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getPassword(), originalConfig.getPassword(),
        "Password should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getComponentType(), originalConfig.getComponentType(),
        "Component type should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getRoleType(), originalConfig.getRoleType(),
        "Role type should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getTables(), originalConfig.getTables(),
        "Tables should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getExcludeTables(), originalConfig.getExcludeTables(),
        "Exclude tables should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getPermissios().size(), originalConfig.getPermissios().size(),
        "Permissions size should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getRlsFilters().size(), originalConfig.getRlsFilters().size(),
        "RLS filters size should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getRlsFilters().get("table1"),
        originalConfig.getRlsFilters().get("table1"), "RLS filters for table1 should match after round-trip");
    Assert.assertEquals(reconstructedConfig.getRlsFilters().get("table2"),
        originalConfig.getRlsFilters().get("table2"), "RLS filters for table2 should match after round-trip");
    // Verify actual filter values
    Assert.assertEquals(reconstructedConfig.getRlsFilters().get("table1").get(0), "country='US'");
    Assert.assertEquals(reconstructedConfig.getRlsFilters().get("table1").get(1), "department='Engineering'");
    Assert.assertEquals(reconstructedConfig.getRlsFilters().get("table2").get(0), "region='WEST'");
  }

  @Test
  public void testDifferentComponentTypes() throws JsonProcessingException {
    ComponentType[] componentTypes = {
        ComponentType.CONTROLLER, ComponentType.BROKER, ComponentType.SERVER,
        ComponentType.MINION, ComponentType.PROXY
    };

    for (ComponentType componentType : componentTypes) {
      UserConfig userConfig = new UserConfigBuilder()
          .setUsername(TEST_USERNAME)
          .setPassword(TEST_PASSWORD)
          .setComponentType(componentType)
          .setRoleType(RoleType.USER)
          .build();

      ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);
      UserConfig reconstructedConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

      Assert.assertEquals(reconstructedConfig.getComponentType(), componentType,
          "Component type should match for " + componentType);
    }
  }

  @Test
  public void testDifferentRoleTypes() throws JsonProcessingException {
    RoleType[] roleTypes = {RoleType.ADMIN, RoleType.USER};

    for (RoleType roleType : roleTypes) {
      UserConfig userConfig = new UserConfigBuilder()
          .setUsername(TEST_USERNAME)
          .setPassword(TEST_PASSWORD)
          .setComponentType(ComponentType.BROKER)
          .setRoleType(roleType)
          .build();

      ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);
      UserConfig reconstructedConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

      Assert.assertEquals(reconstructedConfig.getRoleType(), roleType, "Role type should match for " + roleType);
    }
  }

  @Test
  public void testAllAccessTypes() throws JsonProcessingException {
    List<AccessType> allPermissions = Arrays.asList(
        AccessType.CREATE, AccessType.READ, AccessType.UPDATE, AccessType.DELETE
    );

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.ADMIN)
        .setPermissionList(allPermissions)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);
    UserConfig reconstructedConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);

    Assert.assertEquals(reconstructedConfig.getPermissios().size(), 4, "Should have all 4 access types");
    Assert.assertTrue(reconstructedConfig.getPermissios().contains(AccessType.CREATE), "Should contain CREATE");
    Assert.assertTrue(reconstructedConfig.getPermissios().contains(AccessType.READ), "Should contain READ");
    Assert.assertTrue(reconstructedConfig.getPermissios().contains(AccessType.UPDATE), "Should contain UPDATE");
    Assert.assertTrue(reconstructedConfig.getPermissios().contains(AccessType.DELETE), "Should contain DELETE");
  }

  @Test
  public void testEmptyRlsFiltersMap() throws JsonProcessingException {
    // Empty map should not be serialized
    Map<String, List<String>> emptyRlsFilters = new HashMap<>();

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .setRlsFilters(emptyRlsFilters)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    // Empty map should not be stored in ZNRecord
    Assert.assertNull(znRecord.getSimpleField(UserConfig.RLS_FILTERS_KEY),
        "Empty RLS filters map should not be stored");
  }

  @Test
  public void testZNRecordStructure() throws JsonProcessingException {
    List<String> tableList = Arrays.asList("table1", "table2");
    List<AccessType> permissionList = Arrays.asList(AccessType.READ, AccessType.UPDATE);
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("filter1"));

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .setTableList(tableList)
        .setPermissionList(permissionList)
        .setRlsFilters(rlsFilters)
        .build();

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);

    // Verify structure: simpleFields should contain username, password, component, role, and RLS filters (as JSON)
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    Assert.assertTrue(simpleFields.containsKey(UserConfig.USERNAME_KEY), "Simple fields should contain username");
    Assert.assertTrue(simpleFields.containsKey(UserConfig.PASSWORD_KEY), "Simple fields should contain password");
    Assert.assertTrue(simpleFields.containsKey(UserConfig.COMPONET_KEY), "Simple fields should contain component");
    Assert.assertTrue(simpleFields.containsKey(UserConfig.ROLE_KEY), "Simple fields should contain role");
    Assert.assertTrue(simpleFields.containsKey(UserConfig.RLS_FILTERS_KEY),
        "Simple fields should contain RLS filters as JSON");

    // Verify structure: listFields should contain tables and permissions
    Map<String, List<String>> listFields = znRecord.getListFields();
    Assert.assertTrue(listFields.containsKey(UserConfig.TABLES_KEY), "List fields should contain tables");
    Assert.assertTrue(listFields.containsKey(UserConfig.PERMISSIONS_KEY), "List fields should contain permissions");
  }
}
