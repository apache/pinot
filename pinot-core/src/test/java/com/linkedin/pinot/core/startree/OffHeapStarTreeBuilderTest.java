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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math.util.MathUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OffHeapStarTreeBuilderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "OffHeapStarTreeBuilderTest");

  private void testSimpleCore(int numDimensions, int numMetrics, int numSkipMaterializationDimensions)
      throws Exception {
    int ROWS = (int) MathUtils.factorial(numDimensions);
    Schema schema = new Schema();
    List<String> dimensionsSplitOrder = new ArrayList<>();
    Set<String> skipMaterializationDimensions = new HashSet<>();
    for (int i = 0; i < numDimensions; i++) {
      String dimName = "d" + (i + 1);
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimName, DataType.STRING, true);
      schema.addField(dimensionFieldSpec);

      if (i < (numDimensions - numSkipMaterializationDimensions)) {
        dimensionsSplitOrder.add(dimName);
      } else {
        skipMaterializationDimensions.add(dimName);
      }
    }

    schema.addField(new TimeFieldSpec("daysSinceEpoch", DataType.INT, TimeUnit.DAYS));
    for (int i = 0; i < numMetrics; i++) {
      String metricName = "m" + (i + 1);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, DataType.INT);
      schema.addField(metricFieldSpec);
    }
    StarTreeBuilderConfig builderConfig = new StarTreeBuilderConfig();
    builderConfig.setOutDir(TEMP_DIR);
    builderConfig.setSchema(schema);
    builderConfig.setDimensionsSplitOrder(dimensionsSplitOrder);
    builderConfig.setSkipMaterializationDimensions(skipMaterializationDimensions);
    builderConfig.setMaxNumLeafRecords(10);

    OffHeapStarTreeBuilder builder = new OffHeapStarTreeBuilder();
    builder.init(builderConfig);
    HashMap<String, Object> map = new HashMap<>();
    for (int row = 0; row < ROWS; row++) {
      for (int i = 0; i < numDimensions; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + row % (numDimensions - i));
      }
      //time
      map.put("daysSinceEpoch", 1);
      for (int i = 0; i < numMetrics; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, 1);
      }
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      builder.append(genericRow);
    }
    builder.build();
    int totalDocs = builder.getTotalRawDocumentCount() + builder.getTotalAggregateDocumentCount();
    Iterator<GenericRow> iterator = builder.iterator(builder.getTotalRawDocumentCount(), totalDocs);
    while (iterator.hasNext()) {
      GenericRow row = iterator.next();
      for (String skipMaterializationDimension : skipMaterializationDimensions) {
        String rowValue = (String) row.getValue(skipMaterializationDimension);
        Assert.assertEquals(rowValue, "null");
      }
    }

    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * Test the star tree builder.
   * @throws Exception
   */
  @Test
  public void testSimple() throws Exception {
    testSimpleCore(3, 2, 0);
  }

  /**
   * Test the star tree builder with some dimensions to be skipped from materialization.
   * @throws Exception
   */
  @Test
  public void testSkipMaterialization() throws Exception {
    testSimpleCore(6, 2, 2);
  }
}
