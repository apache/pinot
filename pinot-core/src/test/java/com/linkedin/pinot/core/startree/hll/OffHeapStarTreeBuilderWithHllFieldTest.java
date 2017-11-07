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
package com.linkedin.pinot.core.startree.hll;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.startree.OffHeapStarTreeBuilder;
import com.linkedin.pinot.core.startree.StarTreeBuilderConfig;
import com.linkedin.pinot.startree.hll.HllConstants;
import com.linkedin.pinot.startree.hll.HllSizeUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OffHeapStarTreeBuilderWithHllFieldTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapStarTreeBuilderWithHllFieldTest.class);
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "OffHeapStarTreeBuilderWithHllFieldTest");
  private static final long randomSeed = 31; // a fixed value

  private final String memberIdFieldName = "id";
  private final String hllDeriveFieldSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;
  private final int log2m = 8; //HllUtil.Constants.DEFAULT_LOG2M;

  private void testSimpleCore(int numDimensions, int numMetrics, int numSkipMaterializationDimensions,
      int[] memberIdColumnValues, long preciseCardinality) throws Exception {
    Schema schema = new Schema();
    List<String> dimensionsSplitOrder = new ArrayList<>();
    Set<String> skipMaterializationDimensions = new HashSet<>();

    // add member id dimension spec
    String dimName = memberIdFieldName;
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimName, DataType.INT, true);
    schema.addField(dimensionFieldSpec);
    // add other dimension specs
    for (int i = 1; i < numDimensions; i++) {
      dimName = "d" + (i + 1);
      dimensionFieldSpec = new DimensionFieldSpec(dimName, DataType.STRING, true);
      schema.addField(dimensionFieldSpec);

      if (i < (numDimensions - numSkipMaterializationDimensions)) {
        dimensionsSplitOrder.add(dimName);
      } else {
        skipMaterializationDimensions.add(dimName);
      }
    }

    schema.addField(new TimeFieldSpec("daysSinceEpoch", DataType.INT, TimeUnit.DAYS));
    // add other metric specs
    for (int i = 0; i < numMetrics - 1; i++) {
      String metricName = "m" + (i + 1);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, DataType.INT);
      schema.addField(metricFieldSpec);
    }
    // add hll metric
    String hllMetricName = memberIdFieldName + hllDeriveFieldSuffix;
    MetricFieldSpec hllDerivedFieldSpec =
        new MetricFieldSpec(hllMetricName, FieldSpec.DataType.STRING, HllSizeUtils.getHllFieldSizeFromLog2m(log2m),
            MetricFieldSpec.DerivedMetricType.HLL);
    schema.addField(hllDerivedFieldSpec);

    StarTreeBuilderConfig builderConfig = new StarTreeBuilderConfig();
    builderConfig.setOutDir(TEMP_DIR);
    builderConfig.setSchema(schema);
    builderConfig.setDimensionsSplitOrder(dimensionsSplitOrder);
    builderConfig.setSkipMaterializationDimensions(skipMaterializationDimensions);
    builderConfig.setMaxNumLeafRecords(10);

    OffHeapStarTreeBuilder builder = new OffHeapStarTreeBuilder();
    builder.init(builderConfig);
    // fill values
    HashMap<String, Object> map = new HashMap<>();
    for (int row = 0; row < memberIdColumnValues.length; row++) {
      // add member id column
      dimName = memberIdFieldName;
      map.put(dimName, memberIdColumnValues[row]);
      // add other dimensions
      for (int i = 1; i < numDimensions; i++) {
        dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + row % (numDimensions - i));
      }
      // add time column
      map.put("daysSinceEpoch", 1);
      // add other metrics
      for (int i = 0; i < numMetrics - 1; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, 1);
      }
      // add hll column value
      map.put(hllMetricName, HllUtil.singleValueHllAsString(log2m, memberIdColumnValues[row]));
      //
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      builder.append(genericRow);
    }
    builder.build();

    int totalDocs = builder.getTotalRawDocumentCount() + builder.getTotalAggregateDocumentCount();
    Iterator<GenericRow> iterator = builder.iterator(0, totalDocs);
    while (iterator.hasNext()) {
      GenericRow row = iterator.next();
      LOGGER.info(HllUtil.inspectGenericRow(row, hllDeriveFieldSuffix));
    }

    iterator = builder.iterator(builder.getTotalRawDocumentCount(), totalDocs);
    GenericRow lastRow = null;
    while (iterator.hasNext()) {
      GenericRow row = iterator.next();
      for (String skipMaterializationDimension : skipMaterializationDimensions) {
        String rowValue = (String) row.getValue(skipMaterializationDimension);
        Assert.assertEquals(rowValue, "null");
      }
      lastRow = row;
    }

    assertApproximation(HllUtil.convertStringToHll((String) lastRow.getValue(hllMetricName)).cardinality(),
        preciseCardinality, 0.1);

    FileUtils.deleteDirectory(TEMP_DIR);
  }

  private static void assertApproximation(double estimate, double actual, double precision) {
    estimate = Math.abs(estimate);
    actual = Math.abs(actual);

    double errorRate = 1;
    if (actual > 0) {
      errorRate = Math.abs((actual - estimate) / actual);
    }
    LOGGER.info("estimate: " + estimate + " actual: " + actual + " error (in rate): " + errorRate);
    Assert.assertEquals(errorRate < precision, true);
  }

  private static class RandomNumberArray {
    private static Random _rnd = new Random(randomSeed);

    private final int[] arr;
    private final HashSet<Integer> set = new HashSet<Integer>();

    /**
     * Data ranges between [0, size)
     * @param size
     * @param duplicationPerItem
     */
    RandomNumberArray(int size, int duplicationPerItem) {
      List<Integer> lst = new ArrayList<Integer>();
      for (int i = 0; i < size / duplicationPerItem; i++) {
        Integer item = _rnd.nextInt(size);
        for (int j = 0; j < duplicationPerItem; j++) {
          lst.add(item); // add duplicates
        }
      }
      // add remaining items
      int st = lst.size();
      for (int i = st; i < size; i++) {
        Integer item = _rnd.nextInt(size);
        lst.add(item);
      }
      // add to set
      set.addAll(lst);
      // shuffle
      Collections.shuffle(lst, new Random(10L));
      // toIntArray
      arr = convertToIntArray(lst);
      if (arr.length != size) {
        throw new RuntimeException("should not happen");
      }
    }

    private int[] convertToIntArray(List<Integer> list) {
      int[] ret = new int[list.size()];
      for (int i = 0; i < ret.length; i++) {
        ret[i] = list.get(i);
      }
      return ret;
    }

    public int[] toIntArray() {
      return arr;
    }

    public int size() {
      return arr.length;
    }

    public int getPreciseCardinality() {
      return set.size();
    }
  }

  @Test
  public void testSmallDuplicates() throws Exception {
    RandomNumberArray rand = new RandomNumberArray(500, 1);
    testSimpleCore(3, 3, 0, rand.toIntArray(), rand.getPreciseCardinality());
  }

  @Test
  public void testMediumDuplicates() throws Exception {
    RandomNumberArray rand = new RandomNumberArray(500, 5);
    testSimpleCore(3, 3, 0, rand.toIntArray(), rand.getPreciseCardinality());
  }

  @Test
  public void testLargeDuplicates() throws Exception {
    RandomNumberArray rand = new RandomNumberArray(500, 50);
    testSimpleCore(3, 3, 0, rand.toIntArray(), rand.getPreciseCardinality());
  }

  @Test
  public void testSkipMaterialization() throws Exception {
    RandomNumberArray rand = new RandomNumberArray(250, 3);
    testSimpleCore(6, 4, 2, rand.toIntArray(), rand.getPreciseCardinality());
  }
}
