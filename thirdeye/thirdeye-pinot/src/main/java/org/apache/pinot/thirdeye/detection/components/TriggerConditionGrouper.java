/*
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
package org.apache.pinot.thirdeye.detection.components;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.spec.TriggerConditionGrouperSpec;
import org.apache.pinot.thirdeye.detection.spi.components.Grouper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;


/**
 * Expression based grouper - supports AND, OR and nested combinations of grouping
 */
@Components(title = "TriggerCondition", type = "GROUPER",
    tags = {DetectionTag.GROUPER}, description = "An expression based grouper")
public class TriggerConditionGrouper implements Grouper<TriggerConditionGrouperSpec> {
  protected static final Logger LOG = LoggerFactory.getLogger(TriggerConditionGrouper.class);

  private String expression;
  private String operator;
  private Map<String, Object> leftOp;
  private Map<String, Object> rightOp;
  private InputDataFetcher dataFetcher;

  static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";
  static final String PROP_AND = "and";
  static final String PROP_OR = "or";
  static final String PROP_OPERATOR = "operator";
  static final String PROP_LEFT_OP = "leftOp";
  static final String PROP_RIGHT_OP = "rightOp";

  /**
   * Group based on 'AND' criteria - Entity has anomaly if both sub-entities A and B have anomalies
   * at the same time. This means we find anomaly overlapping interval.
   *
   * Since the anomalies from the respective entities/metrics are merged
   * before calling the grouper, we do not have to deal with overlapping
   * anomalies within an entity/metric
   *
   * Sort anomalies and incrementally compare two anomalies for overlap criteria; break when no overlap
   */
  private List<MergedAnomalyResultDTO> andGrouping(
      List<MergedAnomalyResultDTO> anomalyListA, List<MergedAnomalyResultDTO> anomalyListB) {
    Set<MergedAnomalyResultDTO> groupedAnomalies = new HashSet<>();
    List<MergedAnomalyResultDTO> anomalies = mergeAndSortAnomalies(anomalyListA, anomalyListB);
    if (anomalies.isEmpty()) {
      return anomalies;
    }

    for (int i = 0; i < anomalies.size(); i++) {
      for (int j = i + 1; j < anomalies.size(); j++) {
        // Check for overlap and output it
        if (anomalies.get(j).getStartTime() <= anomalies.get(i).getEndTime()) {
          MergedAnomalyResultDTO currentAnomaly = makeParentEntityAnomaly(anomalies.get(i));
          currentAnomaly.setEndTime(Math.min(currentAnomaly.getEndTime(), anomalies.get(j). getEndTime()));
          currentAnomaly.setStartTime(anomalies.get(j). getStartTime());
          setEntityChildMapping(currentAnomaly, anomalies.get(j));

          groupedAnomalies.add(currentAnomaly);
        } else {
          break;
        }
      }
    }

    return new ArrayList<>(groupedAnomalies);
  }

  /**
   * Group based on 'OR' criteria - Entity has anomaly if either sub-entity A or B have anomalies.
   * This means we find the total anomaly coverage.
   *
   * Since the anomalies from the respective entities/metrics are merged
   * before calling the grouper, we do not have to deal with overlapping
   * anomalies within an entity/metric
   *
   * Sort anomalies by start time and incrementally merge anomalies
   */
  private List<MergedAnomalyResultDTO> orGrouping(
      List<MergedAnomalyResultDTO> anomalyListA, List<MergedAnomalyResultDTO> anomalyListB) {
    Set<MergedAnomalyResultDTO> groupedAnomalies = new HashSet<>();
    List<MergedAnomalyResultDTO> anomalies = mergeAndSortAnomalies(anomalyListA, anomalyListB);
    if (anomalies.isEmpty()) {
      return anomalies;
    }

    MergedAnomalyResultDTO currentAnomaly = makeParentEntityAnomaly(anomalies.get(0));
    for (int i = 1; i < anomalies.size();  i++) {
      if (anomalies.get(i).getStartTime() <= currentAnomaly.getEndTime()) {
        // Partial or full overlap
        currentAnomaly.setEndTime(Math.max(anomalies.get(i).getEndTime(), currentAnomaly.getEndTime()));
        setEntityChildMapping(currentAnomaly, anomalies.get(i));
      }  else {
        // No overlap
        groupedAnomalies.add(currentAnomaly);
        currentAnomaly = makeParentEntityAnomaly(anomalies.get(i));
      }
    }
    groupedAnomalies.add(currentAnomaly);

    return new ArrayList<>(groupedAnomalies);
  }

  /**
   * Groups the anomalies based on the parsed operator tree
   */
  private List<MergedAnomalyResultDTO> groupAnomaliesByOperator(Map<String, Object> operatorNode, List<MergedAnomalyResultDTO> anomalies) {
    Preconditions.checkNotNull(operatorNode);

    // Base condition - If reached leaf node, then return the anomalies corresponding to the entity/metric
    String value = MapUtils.getString(operatorNode, "value");
    if (value != null) {
      return anomalies.stream().filter(anomaly ->
          anomaly.getProperties() != null && anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME) != null
              && anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME).startsWith(value)
      ).collect(Collectors.toList());
    }

    String operator = MapUtils.getString(operatorNode, PROP_OPERATOR);
    Preconditions.checkNotNull(operator, "No operator provided!");
    Map<String, Object> leftOp = ConfigUtils.getMap(operatorNode.get(PROP_LEFT_OP));
    Map<String, Object> rightOp = ConfigUtils.getMap(operatorNode.get(PROP_RIGHT_OP));

    // Post-order traversal - find anomalies from left subtree and right sub-tree and then group them
    List<MergedAnomalyResultDTO> leftAnomalies = groupAnomaliesByOperator(leftOp, anomalies);
    List<MergedAnomalyResultDTO> rightAnomalies = groupAnomaliesByOperator(rightOp, anomalies);
    if (operator.equalsIgnoreCase(PROP_AND)) {
      return andGrouping(leftAnomalies, rightAnomalies);
    } else if (operator.equalsIgnoreCase(PROP_OR)) {
      return orGrouping(leftAnomalies, rightAnomalies);
    } else {
      throw new RuntimeException("Unsupported operator");
    }
  }

  /**
   * Groups the anomalies based on the operator string expression
   */
  private List<MergedAnomalyResultDTO> groupAnomaliesByExpression(String expression, List<MergedAnomalyResultDTO> anomalies) {
    groupAnomaliesByOperator(buildOperatorTree(expression), anomalies);
    return anomalies;
  }

  // TODO: Build parse tree from string expression and execute
  private Map<String, Object> buildOperatorTree(String expression) {
    return new HashMap<>();
  }

  @Override
  public List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies) {
    if (operator != null) {
      Map<String, Object> operatorTreeRoot = new HashMap<>();
      operatorTreeRoot.put(PROP_OPERATOR, operator);
      operatorTreeRoot.put(PROP_LEFT_OP, leftOp);
      operatorTreeRoot.put(PROP_RIGHT_OP, rightOp);
      return groupAnomaliesByOperator(operatorTreeRoot, anomalies);
    } else {
      return groupAnomaliesByExpression(expression, anomalies);
    }
  }

  @Override
  public void init(TriggerConditionGrouperSpec spec, InputDataFetcher dataFetcher) {
    this.expression = spec.getExpression();
    this.operator = spec.getOperator();
    this.leftOp = spec.getLeftOp();
    this.rightOp = spec.getRightOp();
    this.dataFetcher = dataFetcher;
  }
}
