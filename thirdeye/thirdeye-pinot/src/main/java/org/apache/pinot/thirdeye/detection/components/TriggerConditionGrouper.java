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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
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

  private static final String PROP_OPERATOR = "operator";
  private static final String PROP_LEFT_OP = "leftOp";
  private static final String PROP_RIGHT_OP = "rightOp";

  /**
   * Group based on 'AND' criteria - Entity has anomaly if both sub-entities A and B have anomalies
   * at the same time. This means we find anomaly overlapping interval.
   *
   * Since the anomalies from the respective entities/metrics are merged
   * before calling the grouper, we do not have to deal with overlapping
   * anomalies within an entity/metric
   *
   * Core logic:
   * Sort anomalies by start time and iterate through the list
   * Push first anomaly to stack
   * For each anomaly following, pop stack and compare with current anomaly
   * 1. No overlap:
   *    If the anomaly doesn't overlap, create and push new entity anomaly
   * 2. Partial overlap:
   *    If partial overlap, create new grouped entity anomaly and push to stack followed by current anomaly
   * 3. Full overlap:
   *    If full overlap, create new grouped entity anomaly and push to stack followed by current anomaly
   * After the last anomaly, the stack will have one extra anomaly which needs to be popped out.
   * Now the stack will hold all the grouped/entity anomalies
   *
   * Note the each time an entity copy is created, the children needs to be marked and set appropriately
   *
   */
  private List<MergedAnomalyResultDTO> andGrouping(
      List<MergedAnomalyResultDTO> anomalyListA, List<MergedAnomalyResultDTO> anomalyListB) {
    Set<MergedAnomalyResultDTO> groupedAnomalies = new HashSet<>();
    List<MergedAnomalyResultDTO> anomalies = mergeAndSortAnomalies(anomalyListA, anomalyListB);

    Stack<MergedAnomalyResultDTO> anomalyStack = new Stack<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomalyStack.isEmpty()) {
        anomalyStack.push(makeParentEntityAnomaly(anomaly));
        continue;
      }

      MergedAnomalyResultDTO anomalyStackTop = anomalyStack.pop();

      if (anomaly.getStartTime() > anomalyStackTop.getEndTime()) {
        // No overlap
        anomalyStack.push(makeParentEntityAnomaly(anomaly));
      } else if (anomaly.getEndTime() > anomalyStackTop.getEndTime()) {
        //Partial overlap
        anomalyStackTop.setStartTime(anomaly.getStartTime());
        setEntityChildMapping(anomalyStackTop, anomaly);

        anomalyStack.push(anomalyStackTop);
        anomalyStack.push(makeParentEntityAnomaly(anomaly));
      } else {
        // Complete overlap
        MergedAnomalyResultDTO temp = makeEntityCopy(anomalyStackTop);

        anomalyStackTop.setStartTime(anomaly.getStartTime());
        anomalyStackTop.setEndTime(anomaly.getEndTime());
        setEntityChildMapping(anomalyStackTop, anomaly);

        anomalyStack.push(anomalyStackTop);
        anomalyStack.push(temp);
      }
    }

    // Pop the extra anomaly out
    if (!anomalyStack.isEmpty()) {
      anomalyStack.pop();
    }

    // Add all parent anomalies along with their children
    groupedAnomalies.addAll(anomalyStack);
    for (MergedAnomalyResultDTO anomaly : anomalyStack) {
      groupedAnomalies.addAll(anomaly.getChildren());
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
   * Sort anomalies by start time and iterate through the list
   * Create new entity anomaly with first anomaly as child and push to stack
   * For each anomaly following, pop stack and compare
   * 1. No overlap:
   *    If the anomaly doesn't overlap, push popped anomaly, create new entity anomaly with current as child and push
   * 2. Partial overlap:
   *    If partial overlap, update popped anomaly's end time, child and push to stack
   * 3. Full overlap:
   *    If full overlap, update popped anomaly with child info and push it back to stack
   * Note the each time an entity copy is created, the children needs to be marked and set appropriately
   *
   */
  private List<MergedAnomalyResultDTO> orGrouping(
      List<MergedAnomalyResultDTO> anomalyListA, List<MergedAnomalyResultDTO> anomalyListB) {
    Set<MergedAnomalyResultDTO> groupedAnomalies = new HashSet<>();

    List<MergedAnomalyResultDTO> anomalies = mergeAndSortAnomalies(anomalyListA, anomalyListB);

    Stack<MergedAnomalyResultDTO> anomalyStack = new Stack<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomalyStack.isEmpty()) {
        MergedAnomalyResultDTO newEntityAnomaly = makeEntityAnomaly();
        newEntityAnomaly.setStartTime(anomaly.getStartTime());
        newEntityAnomaly.setEndTime(anomaly.getEndTime());
        setEntityChildMapping(newEntityAnomaly, anomaly);
        anomalyStack.push(newEntityAnomaly);
        continue;
      }

      MergedAnomalyResultDTO anomalyStackTop = anomalyStack.pop();

      if (anomaly.getStartTime() > anomalyStackTop.getEndTime()) {
        // No overlap
        anomalyStack.push(anomalyStackTop);

        MergedAnomalyResultDTO newEntityAnomaly = makeEntityAnomaly();
        newEntityAnomaly.setStartTime(anomaly.getStartTime());
        newEntityAnomaly.setEndTime(anomaly.getEndTime());
        setEntityChildMapping(newEntityAnomaly, anomaly);
        anomalyStack.push(newEntityAnomaly);
      } else if (anomaly.getEndTime() > anomalyStackTop.getEndTime()) {
        //Partial overlap
        anomalyStackTop.setEndTime(anomaly.getEndTime());
        setEntityChildMapping(anomalyStackTop, anomaly);

        anomalyStack.push(anomalyStackTop);
      } else {
        // Complete overlap
        setEntityChildMapping(anomalyStackTop, anomaly);

        anomalyStack.push(anomalyStackTop);
      }
    }

    // Add all parent anomalies along with their children
    groupedAnomalies.addAll(anomalyStack);
    for (MergedAnomalyResultDTO anomaly : anomalyStack) {
      groupedAnomalies.addAll(anomaly.getChildren());
    }

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
      List<MergedAnomalyResultDTO> filteredAnomalies = new ArrayList<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        if (anomaly.getProperties() != null && anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME) != null
            && anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME).startsWith(value)) {
          filteredAnomalies.add(anomaly);
        }
      }
      return filteredAnomalies;
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
