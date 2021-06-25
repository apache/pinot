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
package org.apache.pinot.controller.tuner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.DEFAULT_TABLE_CONFIG_TUNER_PACKAGES;


public class RealTimeAutoIndexTunerTest {

  private static final String TABLE_NAME = "test_table";
  private static final String TUNER_NAME = "realtimeAutoIndexTuner";
  private TunerConfig _tunerConfig;
  private Schema schema;
  private String dimensionColumns[] = {"col1", "col2"};
  private String metricColumns[] = {"count"};

  @BeforeClass
  public void setup() {
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(dimensionColumns[0], FieldSpec.DataType.STRING)
        .addSingleValueDimension(dimensionColumns[1], FieldSpec.DataType.STRING)
        .addMetric(metricColumns[0], FieldSpec.DataType.INT).build();
    Map<String, String> props = new HashMap<>();
    _tunerConfig = new TunerConfig(TUNER_NAME, props);
  }

  @Test
  public void testTuner() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTunerConfig(_tunerConfig).build();
    TableConfigTunerRegistry.init(Arrays.asList(DEFAULT_TABLE_CONFIG_TUNER_PACKAGES));
    TableConfigTuner tuner = TableConfigTunerRegistry.getTuner(TUNER_NAME);
    tuner.init(_tunerConfig, schema);
    TableConfig result = tuner.apply(tableConfig);

    IndexingConfig newConfig = result.getIndexingConfig();
    List<String> invertedIndexColumns = newConfig.getInvertedIndexColumns();
    Assert.assertTrue(invertedIndexColumns.size() == 2);
    for (int i = 0; i < dimensionColumns.length; i++) {
      Assert.assertTrue(invertedIndexColumns.contains(dimensionColumns[i]));
    }

    List<String> noDictionaryColumns = newConfig.getNoDictionaryColumns();
    Assert.assertTrue(noDictionaryColumns.size() == 1);
    Assert.assertEquals(noDictionaryColumns.get(0), metricColumns[0]);
  }
}
