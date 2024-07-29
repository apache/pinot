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
package org.apache.pinot.controller.recommender.rules.impl;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.PartitionRuleParams;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.*;


/**
 * Recommend pinot number table partitions on realtime and/or offline side of the table
 * Recommend column to partition on:
 *  For a table whose QPS < Q (say 200 or 300) NO partitioning is needed
 *  For a table whose latency SLA > L (say 1000ms) NO partitioning is needed.
 *  The partitioned dimension D should appear frequently in EQ or IN, according to the
 *  PartitionSegmentPruner::isPartitionMatch():
 *  Should have sufficient cardinality to guarantee balanced partitioning
 * Number of partitions:
 *  Realtime:
 *    num of partitions for real time should be the same value as the number of kafka partitions
 *  Offline:
 *    **We are assuming one segment per partition per push on offline side and this is generally true in Pinot**
 *    Offline partition num is dependent on the amount of data coming in on a given day.
 *    Should give a reasonable partition value that makes the resulting partition size optimal
 *  Hybrid:
 *    The minimum of Realtime and Offline
 */
public class PinotTablePartitionRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTablePartitionRule.class);
  PartitionRuleParams _params;

  public PinotTablePartitionRule(InputManager input, ConfigManager output) {
    super(input, output);
    _params = input.getPartitionRuleParams();
  }

  @Override
  public void run()
      throws InvalidInputException {
    LOGGER.info("Recommending partition configurations");

    if (_input.getQps()
        < _params._thresholdMinQpsPartition) { //For a table whose QPS < Q (say 200 or 300) NO partitioning is needed.
      LOGGER.info("*Input QPS {} < threshold {}, no partition needed", _input.getQps(),
          _params._thresholdMinQpsPartition);
      return;
    }
    if (_input.getLatencySLA()
        > _params._thresholdMaxLatencySlaPartition) { //For a table whose latency SLA > L (say 1000ms) NO
      // partitioning is needed.
      LOGGER.info("*Input SLA {} > threshold {}, no partition needed", _input.getLatencySLA(),
          _params._thresholdMaxLatencySlaPartition);
      return;
    }



    /*For realtime/hybrid, the number of partitions on realtime Pinot table side is same as number of kafka partitions.
    This is generally the case unless there is a reason for them to be different. We saw only one outlier. So
    generally on
     the realtime side the number of partitions should match with the kafka partitions */

    /*The number of partitions on offline table side is dependent on the amount of data.
     **We are assuming one segment per partition per push on offline side and this is generally true in Pinot**
    For hybrid table, we have seen cases where this value = number of kafka partitions = number of realtime table
    * partitions.
    For hybrid table, we have also seen cases, where the value for offline is lower than realtime since the data
    generated on a given day is low volume and using a high count of number of partitions would lead to too many small
    sized segments since we typically have data from one partition in a segment. So we can conclude on the offline
    * side, if the
    data amount is sufficient, the number of partitions will match with the realtime side. Otherwise the partition
    * size will shrink
    so that data_size_per_push/num_offline_partitions >=  OPTIMAL_SIZE_PER_SEGMENT.*/

    LOGGER.info("*Recommending number of partitions ");
    int numKafkaPartitions = _output.getPartitionConfig().getNumKafkaPartitions();
    boolean isRealtimeTable = _input.getTableType().equalsIgnoreCase(REALTIME);
    boolean isHybridTable = _input.getTableType().equalsIgnoreCase(HYBRID);
    boolean isOfflineTable = _input.getTableType().equalsIgnoreCase(OFFLINE);
    if (isRealtimeTable || isHybridTable) {
      //real time num of partitions should be the same value as the number of kafka partitions
      if (!_input.getOverWrittenConfigs().getPartitionConfig().isNumPartitionsRealtimeOverwritten()) {
        _output.getPartitionConfig().setNumPartitionsRealtime(numKafkaPartitions);
      }
    }
    if (isOfflineTable || isHybridTable) {
      //Offline partition num is dependent on the amount of data coming in on a given day.
      //Using a very high value of numPartitions for small dataset size will result in too many small sized segments.
      //We define have a desirable segment size OPTIMAL_SIZE_PER_SEGMENT
      //Divide the size of data coming in on a given day by OPTIMAL_SIZE_PER_SEGMENT we get the number of partitions.
      if (!_input.getOverWrittenConfigs().getPartitionConfig().isNumPartitionsOfflineOverwritten()) {
        int optimalOfflinePartitions = (int) _output.getSegmentSizeRecommendations().getNumSegments();
        _output.getPartitionConfig().setNumPartitionsOffline(optimalOfflinePartitions);
      }
    }

    if (_input.getOverWrittenConfigs().getPartitionConfig().isPartitionDimensionOverwritten()) {
      return;
    }

    LOGGER.info("*Recommending column to partition");

    double[] weights = new double[_input.getNumDims()];
    _input.getParsedQueries().forEach(query -> {
      Double weight = _input.getQueryWeight(query);
      FixedLenBitset fixedLenBitset = parseQuery(_input.getQueryContext(query));
      LOGGER.debug("fixedLenBitset:{}", fixedLenBitset);
      if (fixedLenBitset != null) {
        for (Integer i : fixedLenBitset.getOffsets()) {
          weights[i] += weight;
        }
      }
    });

    List<Pair<String, Double>> columnNameToWeightPairs = new ArrayList<>();
    for (int i = 0; i < _input.getNumDims(); i++) {
      if (weights[i] > 0) {
        columnNameToWeightPairs.add(Pair.of(_input.intToColName(i), weights[i]));
      }
    }
    if (columnNameToWeightPairs.isEmpty()) {
      return;
    }

    LOGGER.info("**Goodness of column to partition {}", columnNameToWeightPairs);
    int numPartitions = isOfflineTable || isHybridTable ? _output.getPartitionConfig().getNumPartitionsOffline()
        : _output.getPartitionConfig().getNumPartitionsRealtime();
    Optional<String> colNameOpt = findBestColumnForPartitioning(columnNameToWeightPairs, _input::getCardinality,
        _params._thresholdRatioMinDimensionPartitionTopCandidates, numPartitions);
    colNameOpt.ifPresent(colName -> _output.getPartitionConfig().setPartitionDimension(colName));
  }

  @VisibleForTesting
  static Optional<String> findBestColumnForPartitioning(List<Pair<String, Double>> columnNameToWeightPairs,
      Function<String, Double> cardinalityExtractor, double topCandidateRatio, int numPartitions) {
    return columnNameToWeightPairs.stream().filter(colToWeight -> cardinalityExtractor.apply(colToWeight.getLeft())
            > numPartitions * PartitionRule.ACCEPTABLE_CARDINALITY_TO_NUM_PARTITIONS_RATIO)
        .max(Comparator.comparingDouble(Pair::getRight)).map(Pair::getRight).flatMap(maxWeight -> {
          double topCandidatesThreshold = maxWeight * topCandidateRatio;
          return columnNameToWeightPairs.stream().filter(colToWeight -> colToWeight.getRight() > topCandidatesThreshold)
              .max(Comparator.comparingDouble(colToWeight -> cardinalityExtractor.apply(colToWeight.getLeft())))
              .map(Pair::getLeft);
        });
  }

  /**
   * Recursively parse the query and find out the dimensions partitions this query,
   * See PartitionSegmentPruner
   */
  public FixedLenBitset parseQuery(QueryContext queryContext) {
    FilterContext filter = queryContext.getFilter();
    if (filter == null || filter.isConstant()) {
      return FixedLenBitset.IMMUTABLE_EMPTY_SET;
    }

    LOGGER.trace("Parsing Where Clause: {}", filter);
    return parsePredicateList(filter);
  }

  @Nullable
  public FixedLenBitset parsePredicateList(FilterContext filterContext) {
    FilterContext.Type type = filterContext.getType();
    FixedLenBitset ret;
    if (type == FilterContext.Type.AND) {
      // a column can appear in only one sub predicate to partition AND predicate
      ret = mutableEmptySet();
      for (int i = 0; i < filterContext.getChildren().size(); i++) {
        FixedLenBitset childResult = parsePredicateList(filterContext.getChildren().get(i));
        if (childResult != null) {
          ret.union(childResult);
        }
      }
    } else if (type == FilterContext.Type.OR) {
      // a column must appear in each sub predicate to partition OR predicate
      ret = null;
      for (int i = 0; i < filterContext.getChildren().size(); i++) {
        FixedLenBitset childResult = parsePredicateList(filterContext.getChildren().get(i));
        if (childResult != null) {
          ret = (ret == null) ? childResult : ret.intersect(childResult);
        }
      }
    } else if (type == FilterContext.Type.NOT) {
      // partition doesn't help for NOT predicate
      ret = null;
    } else {
      // a for leaf we consider only IN (with literal size < threshold) / EQ
      ret = mutableEmptySet();
      Predicate predicate = filterContext.getPredicate();
      Predicate.Type predicateType = predicate.getType();
      ExpressionContext lhs = predicate.getLhs();
      String colName = lhs.toString();
      if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
        LOGGER.trace("Skipping the function {}", colName);
      } else if (_input.isTimeOrDateTimeColumn(colName)) {
        LOGGER.trace("Skipping the DateTime column {}", colName);
        return null;
      } else if (!_input.isDim(colName)) {
        LOGGER.error("Error: Column {} should not appear in filter, ignoring this", colName);
        return null;
      } else if (!_input.isSingleValueColumn(colName)) { // only SV column can be used as partitioning column
        LOGGER.trace("Skipping the MV column {}", colName);
        return null;
      } else if (predicateType == Predicate.Type.IN) {
        int numValuesSelected;

        InPredicate inPredicate = ((InPredicate) predicate);
        List<String> values = inPredicate.getValues();
        if (values.size() == 1) {
          numValuesSelected = 1;
        } else if (values.get(FIRST).equals(IN_PREDICATE_ESTIMATE_LEN_FLAG)) {
          numValuesSelected = Integer.parseInt(values.get(SECOND));
        } else if (values.get(SECOND).equals(IN_PREDICATE_ESTIMATE_LEN_FLAG)) {
          numValuesSelected = Integer.parseInt(values.get(FIRST));
        } else {
          numValuesSelected = values.size();
        }

        if (numValuesSelected <= _params._thresholdMaxInLength) {
          ret.add(_input.colNameToInt(colName));
        }
      } else if (predicateType == Predicate.Type.EQ) {
        ret.add(_input.colNameToInt(colName));
      }
    }
    LOGGER.debug("ret {}", ret);
    return ret;
  }

  private FixedLenBitset mutableEmptySet() {
    return new FixedLenBitset(_input.getNumDims());
  }
}
