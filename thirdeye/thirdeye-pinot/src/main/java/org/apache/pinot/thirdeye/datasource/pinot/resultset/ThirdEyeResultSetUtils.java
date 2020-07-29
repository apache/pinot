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

package org.apache.pinot.thirdeye.datasource.pinot.resultset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.TimeRangeUtils;
import org.apache.pinot.thirdeye.detection.cache.CacheConfig;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdEyeResultSetUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeResultSetUtils.class);
  private static final String MYSQL = "MySQL";
  private static final String H2 = "H2";
  private static final String PINOT = "Pinot";
  private static final String ONLINE = "Online";

  public static List<String[]> parseResultSets(ThirdEyeRequest request,
      Map<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList,
      String sourceName) throws ExecutionException {

    int numGroupByKeys = 0;
    boolean hasGroupBy = false;
    if (request.getGroupByTimeGranularity() != null) {
      numGroupByKeys += 1;
    }
    if (request.getGroupBy() != null) {
      numGroupByKeys += request.getGroupBy().size();
    }
    if (numGroupByKeys > 0) {
      hasGroupBy = true;
    }
    int numMetrics = request.getMetricFunctions().size();
    int numCols = numGroupByKeys + numMetrics;
    boolean hasGroupByTime = false;
    if (request.getGroupByTimeGranularity() != null) {
      hasGroupByTime = true;
    }

    int position = 0;
    Map<String, String[]> dataMap = new HashMap<>();
    Map<String, Integer> countMap = new HashMap<>();
    for (Map.Entry<MetricFunction, List<ThirdEyeResultSet>> entry : metricFunctionToResultSetList.entrySet()) {

      MetricFunction metricFunction = entry.getKey();

      String dataset = metricFunction.getDataset();
      DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
      TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);

      TimeGranularity dataGranularity = null;
      long startTime = request.getStartTimeInclusive().getMillis();
      DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
      DateTime startDateTime = new DateTime(startTime, dateTimeZone);
      dataGranularity = dataTimeSpec.getDataGranularity();
      boolean isISOFormat = false;
      DateTimeFormatter inputDataDateTimeFormatter = null;
      String timeFormat = dataTimeSpec.getFormat();
      if (timeFormat != null && !timeFormat.equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
        isISOFormat = true;
        inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(dateTimeZone);
      }

      List<ThirdEyeResultSet> resultSets = entry.getValue();
      for (int i = 0; i < resultSets.size(); i++) {
        ThirdEyeResultSet resultSet = resultSets.get(i);
        int numRows = resultSet.getRowCount();
        for (int r = 0; r < numRows; r++) {
          boolean skipRowDueToError = false;
          String[] groupKeys;
          String timestamp = null;
          if (hasGroupBy) {
            groupKeys = new String[resultSet.getGroupKeyLength()];
            for (int grpKeyIdx = 0; grpKeyIdx < resultSet.getGroupKeyLength(); grpKeyIdx++) {
              String groupKeyVal = "";
              try {
                groupKeyVal = resultSet.getGroupKeyColumnValue(r, grpKeyIdx);
              } catch (Exception e) {
                // IGNORE FOR NOW, workaround for Pinot Bug
              }
              if (hasGroupByTime && grpKeyIdx == 0) {
                int timeBucket;
                long millis;
                if (!isISOFormat) {
                  millis = dataGranularity.toMillis(Double.valueOf(groupKeyVal).longValue());
                } else {
                    millis = DateTime.parse(groupKeyVal, inputDataDateTimeFormatter).getMillis();
                }
                if (millis < startTime) {
                  LOG.error("Data point earlier than requested start time {}: {}", new Date(startTime), new Date(millis));
                  skipRowDueToError = true;
                  break;
                }
                timeBucket = TimeRangeUtils
                    .computeBucketIndex(request.getGroupByTimeGranularity(), startDateTime,
                        new DateTime(millis, dateTimeZone));
                groupKeyVal = String.valueOf(timeBucket);
                timestamp = String.valueOf(millis);
              }
              groupKeys[grpKeyIdx] = groupKeyVal;
            }
            if (skipRowDueToError) {
              continue;
            }
          } else {
            groupKeys = new String[] {};
          }
          String compositeGroupKey = StringUtils.join(groupKeys, "|");

          String[] rowValues = dataMap.get(compositeGroupKey);
          if (rowValues == null) {
            // add one to include the timestamp, if applicable
            if (timestamp != null && CacheConfig.getInstance().useCentralizedCache()) {
              rowValues = new String[numCols + 1];
            } else {
              rowValues = new String[numCols];
            }
            Arrays.fill(rowValues, "0");
            System.arraycopy(groupKeys, 0, rowValues, 0, groupKeys.length);
            dataMap.put(compositeGroupKey, rowValues);
          }

          String countKey = compositeGroupKey + "|" + position;
          if (!countMap.containsKey(countKey)) {
            countMap.put(countKey, 0);
          }
          final int aggCount = countMap.get(countKey);
          countMap.put(countKey, aggCount + 1);

          // aggregation of multiple values
          rowValues[groupKeys.length + position + i] = String.valueOf(
              reduce(
                  Double.parseDouble(rowValues[groupKeys.length + position + i]),
                  Double.parseDouble(resultSet.getString(r, 0)),
                  aggCount,
                  metricFunction.getFunctionName(),
                  sourceName
              ));

          if (timestamp != null && CacheConfig.getInstance().useCentralizedCache()) {
            rowValues[rowValues.length - 1] = timestamp;
          }
        }
      }
      position ++;
    }
    List<String[]> rows = new ArrayList<>();
    rows.addAll(dataMap.values());
    return rows;
  }

  public static double reduce(double aggregate, double value, int prevCount, MetricAggFunction aggFunction, String sourceName) {
    if (aggFunction.equals(MetricAggFunction.SUM)) {
      return aggregate + value;
    } else if (aggFunction.equals(MetricAggFunction.AVG) || aggFunction.isPercentile()) {
      return (aggregate * prevCount + value) / (prevCount + 1);
    } else if (aggFunction.equals(MetricAggFunction.MAX)) {
      return Math.max(aggregate, value);
    } else if (aggFunction.equals(MetricAggFunction.COUNT)) { // For all COUNT cases
      return aggregate + value;
    } else {
      throw new IllegalArgumentException(String.format("Unknown aggregation function '%s'", aggFunction));
    }
  }
}
