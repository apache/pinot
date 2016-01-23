/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;


public class TestOffheapStarTreeBuilder {
  @Test
  public void test() throws Exception {
    int ROWS = 100;
    int numDimensions = 6;
    int numMetrics = 6;
    StarTreeBuilderConfig builderConfig = new StarTreeBuilderConfig();
    Schema schema = new Schema();
    builderConfig.splitOrder = new ArrayList<>();
    for (int i = 0; i < numDimensions; i++) {
      String dimName = "d" + (i + 1);
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimName, DataType.INT, true);
      schema.addField(dimName, dimensionFieldSpec);
      builderConfig.splitOrder.add(dimName);
    }
    schema.setTimeFieldSpec(new TimeFieldSpec("daysSinceEpoch", DataType.INT, TimeUnit.DAYS));
    for (int i = 0; i < numMetrics; i++) {
      String metricName = "n" + (i + 1);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, DataType.INT);
      schema.addField(metricName, metricFieldSpec);
    }
    builderConfig.maxLeafRecords = 10;
    builderConfig.schema = schema;
    builderConfig.outDir = new File("/tmp/startree");
    OffHeapStarTreeBuilder builder = new OffHeapStarTreeBuilder();
    builder.init(builderConfig);
    Random r = new Random();
    HashMap<String, Object> map = new HashMap<>();
    for (int row = 0; row < ROWS; row++) {
      for (int i = 0; i < numDimensions; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, r.nextInt((numDimensions - i + 2)));
      }
      //time
      map.put("hoursSinceEpoch", r.nextInt(1000));
      for (int i = 0; i < numMetrics; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, r.nextInt((numDimensions - i + 2)));
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      builder.append(genericRow);
    }
    builder.build();
    FileUtils.deleteDirectory(builderConfig.outDir);
  }

}
