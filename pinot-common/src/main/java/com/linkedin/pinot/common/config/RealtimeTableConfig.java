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
package com.linkedin.pinot.common.config;

import java.io.IOException;

import org.antlr.v4.runtime.misc.Nullable;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;


public class RealtimeTableConfig extends AbstractTableConfig {

  private IndexingConfig indexConfig;

  protected RealtimeTableConfig(String tableName, String tableType,
      SegmentsValidationAndRetentionConfig validationConfig, TenantConfig tenantConfig,
      TableCustomConfig customConfigs, IndexingConfig indexConfig, @Nullable QuotaConfig quotaConfig) {
    super(tableName, tableType, validationConfig, tenantConfig, customConfigs, quotaConfig);
    this.indexConfig = indexConfig;
  }

  @Override
  public IndexingConfig getIndexingConfig() {
    return indexConfig;
  }

  public void setIndexConfig(IndexingConfig indexConfig) {
    this.indexConfig = indexConfig;
  }

  @Override
  public String toString() {
    StringBuilder bld = new StringBuilder(super.toString());
    bld.append(indexConfig.toString());
    return bld.toString();
  }

  @Override
  public JSONObject toJSON() throws JSONException, JsonGenerationException, JsonMappingException, IOException {
    JSONObject ret = new JSONObject();
    ret.put("tableName", tableName);
    ret.put("tableType", tableType);
    ret.put("segmentsConfig", new JSONObject(new ObjectMapper().writeValueAsString(validationConfig)));
    ret.put("tenants", new JSONObject(new ObjectMapper().writeValueAsString(tenantConfig)));
    ret.put("tableIndexConfig", new JSONObject(new ObjectMapper().writeValueAsString(indexConfig)));
    ret.put("metadata", new JSONObject(new ObjectMapper().writeValueAsString(customConfigs)));
    return ret;
  }

}
