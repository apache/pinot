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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TagNameUtilsTest {

  @DataProvider(name = "tagOverrideConfigTestDataProvider")
  public Object[][] tagOverrideConfigTestDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    TenantConfig tenantConfig = new TenantConfig(null, "aServerTenant", null);
    inputs.add(new Object[]{tenantConfig, "aServerTenant_OFFLINE", "aServerTenant_REALTIME", "aServerTenant_REALTIME"});

    // empty tag override
    tenantConfig = new TenantConfig(null, "aServerTenant", new TagOverrideConfig(null, null));
    inputs.add(new Object[]{tenantConfig, "aServerTenant_OFFLINE", "aServerTenant_REALTIME", "aServerTenant_REALTIME"});

    // defined realtime consuming override
    tenantConfig = new TenantConfig(null, "aServerTenant", new TagOverrideConfig("overriddenTag_REALTIME", null));
    inputs.add(new Object[]{tenantConfig, "aServerTenant_OFFLINE", "overriddenTag_REALTIME", "aServerTenant_REALTIME"});

    // defined realtime completed override
    tenantConfig = new TenantConfig(null, "aServerTenant", new TagOverrideConfig(null, "overriddenTag_OFFLINE"));
    inputs.add(new Object[]{tenantConfig, "aServerTenant_OFFLINE", "aServerTenant_REALTIME", "overriddenTag_OFFLINE"});

    // defined both overrides
    tenantConfig = new TenantConfig(null, "aServerTenant", new TagOverrideConfig("overriddenTag_REALTIME", "overriddenTag_OFFLINE"));
    inputs.add(new Object[]{tenantConfig, "aServerTenant_OFFLINE", "overriddenTag_REALTIME", "overriddenTag_OFFLINE"});

    return inputs.toArray(new Object[inputs.size()][]);
  }

  @Test(dataProvider = "tagOverrideConfigTestDataProvider")
  public void testTagOverrideConfig(TenantConfig tenantConfig, String expectedOfflineServerTag, String expectedConsumingServerTag,
      String expectedCompletedServerTag) {
    assertEquals(TagNameUtils.extractOfflineServerTag(tenantConfig), expectedOfflineServerTag);
    assertEquals(TagNameUtils.extractConsumingServerTag(tenantConfig), expectedConsumingServerTag);
    assertEquals(TagNameUtils.extractCompletedServerTag(tenantConfig), expectedCompletedServerTag);
  }
}
