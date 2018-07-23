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
package com.linkedin.pinot.core.startreeV2;

import java.util.List;
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

  public static void printStarTree(StarTree s) {

    printDimensionNames(s.getDimensionNames());
    printNode(s.getRoot());
  }

  public static void printDimensionNames(List<String> dimensionNames) {
    for (int i = 0; i < dimensionNames.size(); i++) {
      System.out.println(dimensionNames.get(i));
    }
    return;
  }

  public static void printNode(StarTreeNode node) {
    System.out.print("Printing Dimension ID: ");
    System.out.println(node.getDimensionId());
    System.out.println("Printing Dimension Value: ");
    System.out.println(node.getDimensionValue());

    return;
  }

  public static void printDimensionDataFromDataSource(DataSource source) {
    Block block = source.nextBlock();
    BlockValSet blockValSet = block.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
    while (itr.hasNext()) {
      System.out.println(itr.nextIntVal());
    }
    return;
  }

  public static void printMetricAggfuncDataFromDataSource(DataSource source, String dataType) throws IOException {
    Block block = source.nextBlock();
    BlockValSet blockValSet = block.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
    AggregationFunctionFactory aggregationFunctionFactory = new AggregationFunctionFactory();

    while (itr.hasNext()) {
      if (dataType.equals(StarTreeV2Constant.AggregateFunctions.SUM)) {
        System.out.println(itr.nextDoubleVal());
      } else if (dataType.equals(StarTreeV2Constant.AggregateFunctions.COUNT)) {
        System.out.println(itr.nextLongVal());
      } else if (dataType.equals(StarTreeV2Constant.AggregateFunctions.MAX)) {
        System.out.println(itr.nextDoubleVal());
      } else if (dataType.equals(StarTreeV2Constant.AggregateFunctions.MIN)) {
        System.out.println(itr.nextDoubleVal());
      } else if (dataType.equals(StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL)) {
        AggregationFunction function = aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL);
        byte[] h = itr.nextBytesVal();
        System.out.println(function.deserialize(h) instanceof HyperLogLog);
        System.out.println(h.length);
      } else if (dataType.equals(StarTreeV2Constant.AggregateFunctions.PERCENTILEEST)) {
        AggregationFunction function = aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.PERCENTILEEST);
        byte[] h = itr.nextBytesVal();
        System.out.println(function.deserialize(h) instanceof QuantileDigest);
        System.out.println(h.length);
      } else if (dataType.equals(StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST)) {
        AggregationFunction function = aggregationFunctionFactory.getAggregationFunction(StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST);
        byte[] h = itr.nextBytesVal();
        System.out.println(function.deserialize(h) instanceof TDigest);
        System.out.println(h.length);
      }
    }
    return;
  }
}
