/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TagOverrideConfigTest {

  @DataProvider(name = "realtimeTagConfigTestDataProvider")
  public Object[][] realtimeTagConfigTestDataProvider() throws IOException, JSONException {
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE);
    tableConfigBuilder.setTableName("testRealtimeTable")
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setServerTenant("aServerTenant");

    List<Object[]> inputs = new ArrayList<>();

    TableConfig tableConfig = tableConfigBuilder.build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_REALTIME", "aServerTenant_REALTIME"});

    tableConfig = tableConfigBuilder.setTagOverrideConfig(null).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_REALTIME", "aServerTenant_REALTIME"});

    // empty tag override
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig();
    tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_REALTIME", "aServerTenant_REALTIME"});

    // defined realtime consuming override
    tagOverrideConfig = new TagOverrideConfig();
    tagOverrideConfig.setRealtimeConsuming("overriddenTag_REALTIME");
    tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "overriddenTag_REALTIME", "aServerTenant_REALTIME"});

    // defined realtime completed override
    tagOverrideConfig = new TagOverrideConfig();
    tagOverrideConfig.setRealtimeCompleted("overriddenTag_OFFLINE");
    tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_REALTIME", "overriddenTag_OFFLINE"});

    // defined both overrides
    tagOverrideConfig = new TagOverrideConfig();
    tagOverrideConfig.setRealtimeConsuming("overriddenTag_REALTIME");
    tagOverrideConfig.setRealtimeCompleted("overriddenTag_OFFLINE");
    tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "overriddenTag_REALTIME", "overriddenTag_OFFLINE"});

    return inputs.toArray(new Object[inputs.size()][]);
  }

  @Test(dataProvider = "realtimeTagConfigTestDataProvider")
  public void testRealtimeTagConfig(TableConfig tableConfig, String expectedServerTenant,
      String expectedRealtimeConsumingTag, String expectedRealtimeCompletedTag) {
    RealtimeTagConfig tagConfig = new RealtimeTagConfig(tableConfig);
    Assert.assertEquals(tagConfig.getServerTenantName(), expectedServerTenant);
    Assert.assertEquals(tagConfig.getConsumingServerTag(), expectedRealtimeConsumingTag);
    Assert.assertEquals(tagConfig.getCompletedServerTag(), expectedRealtimeCompletedTag);
  }

  @DataProvider(name = "offlineTagConfigTestDataProvider")
  public Object[][] offlineTagConfigTestDataProvider() throws IOException, JSONException {
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE);
    tableConfigBuilder.setTableName("testOfflineTable")
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setServerTenant("aServerTenant");

    List<Object[]> inputs = new ArrayList<>();

    TableConfig tableConfig = tableConfigBuilder.build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_OFFLINE"});

    tableConfig = tableConfigBuilder.setTagOverrideConfig(null).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_OFFLINE"});

    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig();
    tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_OFFLINE"});

    tagOverrideConfig = new TagOverrideConfig();
    tagOverrideConfig.setRealtimeConsuming("overriddenTag_REALTIME");
    tagOverrideConfig.setRealtimeCompleted("overriddenTag_OFFLINE");
    tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();
    inputs.add(new Object[] {tableConfig, "aServerTenant", "aServerTenant_OFFLINE"});

    return inputs.toArray(new Object[inputs.size()][]);
  }

  @Test(dataProvider = "offlineTagConfigTestDataProvider")
  public void testOfflineTagConfig(TableConfig tableConfig, String expectedServerTenant, String expectedOfflineServerTag) {
    OfflineTagConfig tagConfig = new OfflineTagConfig(tableConfig);
    Assert.assertEquals(tagConfig.getServerTenantName(), expectedServerTenant);
    Assert.assertEquals(tagConfig.getOfflineServerTag(), expectedOfflineServerTag);
  }

}
