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
package com.linkedin.pinot.core.startree.v2;

import java.util.List;
import org.testng.Assert;
import java.io.IOException;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.startree.StarTreeNode;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;


public class StarTreeV2LoaderHelper {

  static void printStarTree(StarTree s) {

    printDimensionNames(s.getDimensionNames());
    printNode(s.getRoot());
  }

  private static void printDimensionNames(List<String> dimensionNames) {
    for (int i = 0; i < dimensionNames.size(); i++) {
      System.out.println(dimensionNames.get(i));
    }
  }

  private static void printNode(StarTreeNode node) {
    System.out.print("Printing Dimension ID: ");
    System.out.println(node.getDimensionId());
    System.out.println("Printing Dimension Value: ");
    System.out.println(node.getDimensionValue());
  }

  static void printDimensionDataFromDataSource(DataSource source) {
    Block block = source.nextBlock();
    BlockValSet blockValSet = block.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
    while (itr.hasNext()) {
      System.out.println(itr.nextIntVal());
    }
  }

  static void printMetricAggfuncDataFromDataSource(DataSource source, String dataType) throws IOException {
    Block block = source.nextBlock();
    BlockValSet blockValSet = block.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
    AggregationFunctionFactory aggregationFunctionFactory = new AggregationFunctionFactory();

    while (itr.hasNext()) {
      switch (dataType) {
        case StarTreeV2Constant.AggregateFunctions.SUM:
          System.out.println(itr.nextDoubleVal());
          break;
        case StarTreeV2Constant.AggregateFunctions.COUNT:
          System.out.println(itr.nextLongVal());
          break;
        case StarTreeV2Constant.AggregateFunctions.MAX:
          System.out.println(itr.nextDoubleVal());
          break;
        case StarTreeV2Constant.AggregateFunctions.MIN:
          System.out.println(itr.nextDoubleVal());
          break;
        case StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL: {
          AggregationFunction function =
              aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL);
          byte[] h = itr.nextBytesVal();
          System.out.println(function.deserialize(h) instanceof HyperLogLog);
          System.out.println(h.length);
          break;
        }
        case StarTreeV2Constant.AggregateFunctions.PERCENTILEEST: {
          AggregationFunction function =
              aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.PERCENTILEEST);
          byte[] h = itr.nextBytesVal();
          System.out.println(function.deserialize(h) instanceof QuantileDigest);
          System.out.println(h.length);
          break;
        }
        case StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST: {
          AggregationFunction function = aggregationFunctionFactory.getAggregationFunction(
              StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST);
          byte[] h = itr.nextBytesVal();
          System.out.println(function.deserialize(h) instanceof TDigest);
          System.out.println(h.length);
          break;
        }
      }
    }
  }

  static void compareDimensionDataSources(DataSource d1, DataSource d2) {
    Block b1 = d1.nextBlock();
    Block b2 = d2.nextBlock();

    com.linkedin.pinot.core.segment.index.readers.Dictionary dict1 = d1.getDictionary();
    com.linkedin.pinot.core.segment.index.readers.Dictionary dict2 = d2.getDictionary();

    BlockValSet blockValSet1 = b1.getBlockValueSet();
    BlockValSet blockValSet2 = b2.getBlockValueSet();

    BlockSingleValIterator itr1 = (BlockSingleValIterator) blockValSet1.iterator();
    BlockSingleValIterator itr2 = (BlockSingleValIterator) blockValSet2.iterator();

    while (itr1.hasNext() || itr2.hasNext()) {
      int a = itr1.nextIntVal();
      int b = itr2.nextIntVal();
      System.out.println(Integer.toString(a) + ", " + Integer.toString(b));
      Assert.assertEquals(dict1.get(a), dict2.get(b));
    }
  }

  static void compareMetricAggfuncDataFromDataSource(DataSource d1, DataSource d2, String dataType) throws IOException {
    Block b1 = d1.nextBlock();
    BlockValSet blockValSet1 = b1.getBlockValueSet();
    BlockSingleValIterator itr1 = (BlockSingleValIterator) blockValSet1.iterator();

    Block b2 = d2.nextBlock();
    BlockValSet blockValSet2 = b2.getBlockValueSet();
    BlockSingleValIterator itr2 = (BlockSingleValIterator) blockValSet2.iterator();

    AggregationFunctionFactory aggregationFunctionFactory = new AggregationFunctionFactory();

    while (itr1.hasNext() || itr2.hasNext()) {
      switch (dataType) {
        case StarTreeV2Constant.AggregateFunctions.SUM:
        case StarTreeV2Constant.AggregateFunctions.MAX:
        case StarTreeV2Constant.AggregateFunctions.MIN:
          double da = itr1.nextDoubleVal();
          double db = itr2.nextDoubleVal();
          System.out.println(Double.toString(da) + ", " + Double.toString(db));
          Assert.assertEquals(da, db);
          break;

        case StarTreeV2Constant.AggregateFunctions.COUNT:
          long la = itr1.nextLongVal();
          long lb = itr2.nextLongVal();
          System.out.println(Long.toString(la) + ", " + Long.toString(lb));
          Assert.assertEquals(la, lb);
          break;

        case StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL: {
          AggregationFunction function =
              aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL);
          byte[] ah = itr1.nextBytesVal();
          byte[] bh = itr2.nextBytesVal();

          System.out.println((function.deserialize(ah) instanceof HyperLogLog) + ", " + (function.deserialize(bh) instanceof HyperLogLog));
          break;
        }
        case StarTreeV2Constant.AggregateFunctions.PERCENTILEEST: {
          AggregationFunction function =
              aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.PERCENTILEEST);
          byte[] ah = itr1.nextBytesVal();
          byte[] bh = itr2.nextBytesVal();

          System.out.println((function.deserialize(ah) instanceof QuantileDigest) + ", " + (function.deserialize(bh) instanceof QuantileDigest));
          break;
        }
        case StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST: {
          AggregationFunction function = aggregationFunctionFactory.getAggregationFunction(
              StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST);
          byte[] ah = itr1.nextBytesVal();
          byte[] bh = itr2.nextBytesVal();

          System.out.println((function.deserialize(ah) instanceof TDigest) + ", " + (function.deserialize(bh) instanceof TDigest));
          break;
        }
      }
    }
  }
}
