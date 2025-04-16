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
package org.apache.pinot.segment.local.upsert.merger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PartialUpsertMergerFactoryTest {

  @Test
  public void testGetPartialUpsertMerger() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG).addMetric("field2", FieldSpec.DataType.LONG)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.OVERWRITE);
    upsertConfig.setPartialUpsertStrategies(partialUpsertStrategies);
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.IGNORE);
    upsertConfig.setPartialUpsertMergerClass(
        "org.apache.pinot.segment.local.upsert.merger.PartialUpsertColumnarMerger");

    PartialUpsertMerger partialUpsertMerger =
        PartialUpsertMergerFactory.getPartialUpsertMerger(schema.getPrimaryKeyColumns(),
            Collections.singletonList("hoursSinceEpoch"), upsertConfig);

    assertNotNull(partialUpsertMerger);
    assertTrue(partialUpsertMerger instanceof PartialUpsertColumnarMerger);
  }
}
