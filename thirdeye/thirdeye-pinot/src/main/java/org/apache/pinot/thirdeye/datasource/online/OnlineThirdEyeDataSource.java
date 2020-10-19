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

package org.apache.pinot.thirdeye.datasource.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datalayer.bao.OnlineDetectionDataManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OnlineDetectionDataDTO;
import org.apache.pinot.thirdeye.datasource.*;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.*;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Collection;

public class OnlineThirdEyeDataSource implements ThirdEyeDataSource {
  static final Logger LOG = LoggerFactory.getLogger(OnlineThirdEyeDataSource.class);

  public static final String DATA_SOURCE_NAME = OnlineThirdEyeDataSource.class.getSimpleName();
  private static final String ONLINE = "Online";
  private final OnlineDetectionDataManager onlineDetectionDataDAO;
  Map<String, DataFrame> dataSets;

  /**
   * Construct an online data source, which connects to internal database to retrieve data.
   *
   * @param properties the configuration providing the information to initialize online datasource.
   */
  public OnlineThirdEyeDataSource(Map<String, Object> properties) {
    LOG.info("Initializing online datasource with prop: " + properties);

    this.onlineDetectionDataDAO = DAORegistry.getInstance().getOnlineDetectionDataManager();
  }

  private static DateTime convertStringToDate(final String str, String tz, String dateFormat) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
    formatter.withZone(DateTimeZone.forID(tz));
    return formatter.parseDateTime(str);
  }

  @Override public String getName() {
    return DATA_SOURCE_NAME;
  }

  @Override public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    // Currently only support single metric function (non-derived)
    if (request.getMetricFunctions().size() != 1) {
      throw new RuntimeException(
          "Not supported size of metric functions: " + request.getMetricFunctions().size());
    }

    LinkedHashMap<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList =
        new LinkedHashMap<>();

    // Retrieve online data from database
    MetricFunction metricFunction = request.getMetricFunctions().get(0);
    MetricConfigDTO metricConfig = metricFunction.getMetricConfig();

    String dataset = metricFunction.getDataset();
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
    TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);

    List<OnlineDetectionDataDTO> onlineDetectionDataDTOs
        = onlineDetectionDataDAO.findByDatasetAndMetric(dataset, metricConfig.getName());
    Preconditions.checkState(onlineDetectionDataDTOs.size()==1,
        String.format("Find %d online data", onlineDetectionDataDTOs.size()));
    String onlineDetectionData = onlineDetectionDataDTOs.get(0).getOnlineDetectionData();

    String timeColumnName = datasetConfig.getTimeColumn();
    String metricColumnName = metricConfig.getName();

    Multimap<String, String> decoratedFilterSet = request.getFilterSet();

    List<String> columnNameWithDataType = new ArrayList<>();
    columnNameWithDataType.add(timeColumnName + ":STRING");
    columnNameWithDataType.add(metricColumnName + ":STRING");
    DataFrame.Builder dfBuilder = DataFrame.builder(columnNameWithDataType);

    // Find time/metric/filtering column indices
    int totalColumnCount = 2; // Currently only support default settings: <date, online_metric>
    String[] columnsOfTheRow;
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(onlineDetectionData);
    ArrayNode columnsNode = (ArrayNode) rootNode.path("columns");
    ArrayNode rowsNode = (ArrayNode) rootNode.path("rows");

    int timeColIdx = -1, metricColIdx = -1;
    Map<String, Integer> colNameToFilterColIndices = new HashMap<>();
    if (columnsNode.isArray()) {
      int idx = 0;
      for (JsonNode columnNode : columnsNode) {
        String columnName = columnNode.textValue();
        if (columnName.equals(timeColumnName))
          timeColIdx = idx;
        else if (columnName.equals(metricColumnName))
          metricColIdx = idx;
        if (decoratedFilterSet.keySet().contains(columnName))
          colNameToFilterColIndices.put(columnName, idx);
        idx++;
      }
    }

    // Filter values in each row and build dataFrame
    DateTime startTime = request.getStartTimeInclusive();
    DateTime endTime = request.getEndTimeExclusive();

    if (rowsNode.isArray()) {
      for (JsonNode rowNode : rowsNode) {
        if (rowNode.isArray()) {
          // Filter by time range
          String timeValString = rowNode.get(timeColIdx).textValue();
          DateTime timeValDate = convertStringToDate(timeValString, datasetConfig.getTimezone(),
              dataTimeSpec.getFormat());
          if (timeValDate.isBefore(startTime) || timeValDate.isAfter(endTime))
            continue;

          // Filter by filtering set
          boolean needFiltered = false;
          for (String columnName : colNameToFilterColIndices.keySet()) {
            int inspectIdx = colNameToFilterColIndices.get(columnName);
            String inspectVal = rowNode.get(inspectIdx).textValue();
            Collection<String> filterSet = decoratedFilterSet.get(columnName);
            if (filterSet.contains(inspectVal)) {
              needFiltered = true;
              break;
            }
          }
          if (needFiltered)
            continue;

          int idxInDF = 0;
          columnsOfTheRow = new String[totalColumnCount];
          columnsOfTheRow[idxInDF++] = rowNode.get(timeColIdx).textValue();
          columnsOfTheRow[idxInDF] = rowNode.get(metricColIdx).textValue();
          dfBuilder.append(columnsOfTheRow);
        }
      }
    }
    DataFrame dataFrame = dfBuilder.build();

    // Create resultSetGroup
    List<String> groupKeyColumnNames = new ArrayList<>();
    groupKeyColumnNames.add(timeColumnName);
    List<String> metricColumnNames = new ArrayList<>();
    metricColumnNames.add(metricColumnName);
    ThirdEyeResultSetMetaData thirdEyeResultSetMetaData =
        new ThirdEyeResultSetMetaData(groupKeyColumnNames, metricColumnNames);

    List<ThirdEyeResultSet> resultSets = new ArrayList<>();
    resultSets.add(new ThirdEyeDataFrameResultSet(thirdEyeResultSetMetaData, dataFrame));
    ThirdEyeResultSetGroup resultSetGroup = new ThirdEyeResultSetGroup(resultSets);
    metricFunctionToResultSetList.put(metricFunction, resultSetGroup.getResultSets());

    List<String[]> resultRows =
        ThirdEyeResultSetUtils.parseResultSets(request, metricFunctionToResultSetList, ONLINE);

    return new RelationalThirdEyeResponse(request, resultRows, dataTimeSpec);
  }

  @Override public List<String> getDatasets() throws Exception {
    return new ArrayList<>(this.dataSets.keySet());
  }

  @Override public void clear() throws Exception {
    throw new RuntimeException("Online service: not supported");
  }

  @Override public void close() throws Exception {
    throw new RuntimeException("Online service: not supported");
  }

  @Override public long getMaxDataTime(String dataset) throws Exception {
    throw new RuntimeException("Online service: not supported");
  }

  @Override public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    throw new RuntimeException("Online service: not supported");
  }
}
