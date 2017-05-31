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

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import java.io.IOException;
import org.apache.helix.ZNRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableConfigTest {

  @Test
  public void testSerializeDeserialize()
      throws IOException, JSONException {
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable");
    {
      // No quota config
      TableConfig tableConfig = tableConfigBuilder.build();

      Assert.assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      Assert.assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      Assert.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      Assert.assertNull(tableConfig.getQuotaConfig());

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNull(tableConfigToCompare.getQuotaConfig());

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNull(tableConfigToCompare.getQuotaConfig());
    }
    {
      // With quota config
      QuotaConfig quotaConfig = new QuotaConfig();
      quotaConfig.setStorage("30G");
      TableConfig tableConfig = tableConfigBuilder.setQuotaConfig(quotaConfig).build();

      Assert.assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      Assert.assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      Assert.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      Assert.assertNotNull(tableConfig.getQuotaConfig());
      Assert.assertEquals(tableConfig.getQuotaConfig().getStorage(), "30G");

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNotNull(tableConfigToCompare.getQuotaConfig());
      Assert.assertEquals(tableConfigToCompare.getQuotaConfig().getStorage(),
          tableConfig.getQuotaConfig().getStorage());

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNotNull(tableConfigToCompare.getQuotaConfig());
      Assert.assertEquals(tableConfigToCompare.getQuotaConfig().getStorage(),
          tableConfig.getQuotaConfig().getStorage());
    }
  }
}
