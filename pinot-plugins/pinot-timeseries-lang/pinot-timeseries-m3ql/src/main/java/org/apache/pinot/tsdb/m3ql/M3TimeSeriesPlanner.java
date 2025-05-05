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
package org.apache.pinot.tsdb.m3ql;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.m3ql.parser.Tokenizer;
import org.apache.pinot.tsdb.m3ql.plan.KeepLastValuePlanNode;
import org.apache.pinot.tsdb.m3ql.plan.TransformNullPlanNode;
import org.apache.pinot.tsdb.m3ql.time.TimeBucketComputer;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.TimeSeriesMetadata;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class M3TimeSeriesPlanner implements TimeSeriesLogicalPlanner {
  @Override
  public void init(PinotConfiguration pinotConfiguration) {
  }

  @Override
  public TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request, TimeSeriesMetadata tableMetadata) {
    if (!request.getLanguage().equals(Constants.LANGUAGE)) {
      throw new IllegalArgumentException(
          String.format("Invalid engine id: %s. Expected: %s", request.getLanguage(), Constants.LANGUAGE));
    }
    // Step-1: Parse and create a logical plan tree.
    BaseTimeSeriesPlanNode planNode = planQuery(request);
    // Step-2: Compute the time-buckets.
    TimeBuckets timeBuckets = TimeBucketComputer.compute(planNode, request);
    return new TimeSeriesLogicalPlanResult(planNode, timeBuckets);
  }

  public BaseTimeSeriesPlanNode planQuery(RangeTimeSeriesRequest request) {
    PlanIdGenerator planIdGenerator = new PlanIdGenerator();
    Tokenizer tokenizer = new Tokenizer(request.getQuery());
    List<List<String>> commands = tokenizer.tokenize();
    Preconditions.checkState(commands.size() > 1,
        "At least two commands required. " + "Query should start with a fetch followed by an aggregation.");
    BaseTimeSeriesPlanNode lastNode = null;
    AggInfo aggInfo = null;
    List<String> groupByColumns = new ArrayList<>();
    BaseTimeSeriesPlanNode rootNode = null;
    for (int commandId = commands.size() - 1; commandId >= 0; commandId--) {
      String command = commands.get(commandId).get(0);
      Preconditions.checkState(
          (command.equals("fetch") && commandId == 0) || (!command.equals("fetch") && commandId > 0),
          "fetch should be the first command");
      List<BaseTimeSeriesPlanNode> children = new ArrayList<>();
      BaseTimeSeriesPlanNode currentNode = null;
      switch (command) {
        case "fetch":
          List<String> tokens = commands.get(commandId).subList(1, commands.get(commandId).size());
          currentNode = handleFetchNode(planIdGenerator.generateId(), tokens, children, aggInfo, groupByColumns,
              request);
          break;
        case "sum":
        case "min":
        case "max":
          Preconditions.checkState(commandId == 1, "Aggregation should be the second command (fetch should be first)");
          Preconditions.checkState(aggInfo == null, "Aggregation already set. Only single agg allowed.");
          aggInfo = new AggInfo(command.toUpperCase(Locale.ENGLISH), false, Collections.emptyMap());
          if (commands.get(commandId).size() > 1) {
            String[] cols = commands.get(commandId).get(1).split(",");
            groupByColumns = Stream.of(cols).map(String::trim).collect(Collectors.toList());
          }
          break;
        case "keepLastValue":
          currentNode = new KeepLastValuePlanNode(planIdGenerator.generateId(), children);
          break;
        case "transformNull":
          Double defaultValue = TransformNullPlanNode.DEFAULT_VALUE;
          if (commands.get(commandId).size() > 1) {
            defaultValue = Double.parseDouble(commands.get(commandId).get(1));
          }
          currentNode = new TransformNullPlanNode(planIdGenerator.generateId(), defaultValue, children);
          break;
        default:
          throw new IllegalArgumentException("Unknown function: " + command);
      }
      if (currentNode != null) {
        if (rootNode == null) {
          rootNode = currentNode;
        }
        if (lastNode != null) {
          lastNode.addInputNode(currentNode);
        }
        lastNode = currentNode;
      }
    }
    return rootNode;
  }

  public BaseTimeSeriesPlanNode handleFetchNode(String planId, List<String> tokens,
      List<BaseTimeSeriesPlanNode> children, AggInfo aggInfo, List<String> groupByColumns,
      RangeTimeSeriesRequest request) {
    Preconditions.checkState(tokens.size() % 2 == 0, "Mismatched args");
    String tableName = null;
    String timeColumn = null;
    TimeUnit timeUnit = null;
    String filter = "";
    String valueExpr = null;
    for (int idx = 0; idx < tokens.size(); idx += 2) {
      String key = tokens.get(idx);
      String value = tokens.get(idx + 1);
      switch (key) {
        case "table":
          tableName = value.replaceAll("\"", "");
          break;
        case "ts_column":
          timeColumn = value.replaceAll("\"", "");
          break;
        case "ts_unit":
          timeUnit = TimeUnit.valueOf(value.replaceAll("\"", "").toUpperCase(Locale.ENGLISH));
          break;
        case "filter":
          filter = value.replaceAll("\"", "");
          break;
        case "value":
          valueExpr = value.replaceAll("\"", "");
          break;
        default:
          throw new IllegalArgumentException("Unknown key: " + key);
      }
    }
    Preconditions.checkNotNull(tableName, "Table name not set. Set via table=");
    Preconditions.checkNotNull(timeColumn, "Time column not set. Set via time_col=");
    Preconditions.checkNotNull(timeUnit, "Time unit not set. Set via time_unit=");
    Preconditions.checkNotNull(valueExpr, "Value expression not set. Set via value=");
    Map<String, String> queryOptions = new HashMap<>();
    if (request.getNumGroupsLimit() > 0) {
      queryOptions.put("numGroupsLimit", Integer.toString(request.getNumGroupsLimit()));
    }
    return new LeafTimeSeriesPlanNode(planId, children, tableName, timeColumn, timeUnit, 0L, filter, valueExpr, aggInfo,
        groupByColumns, request.getLimit(), queryOptions);
  }
}
