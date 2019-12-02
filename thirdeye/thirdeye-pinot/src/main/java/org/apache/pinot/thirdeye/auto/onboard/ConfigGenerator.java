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

package org.apache.pinot.thirdeye.auto.onboard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.data.TimeGranularitySpec.TimeFormat;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DatasetConfigBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;

public class ConfigGenerator {

  private static final String PDT_TIMEZONE = "US/Pacific";
  private static final String BYTES_STRING = "BYTES";
  private static final String NON_ADDITIVE = "non_additive";
  private static final String PINOT_PRE_AGGREGATED_KEYWORD = "*";

  public static void setTimeSpecs(DatasetConfigDTO datasetConfigDTO, TimeGranularitySpec timeSpec) {
    datasetConfigDTO.setTimeColumn(timeSpec.getName());
    datasetConfigDTO.setTimeDuration(timeSpec.getTimeUnitSize());
    datasetConfigDTO.setTimeUnit(timeSpec.getTimeType());
    datasetConfigDTO.setTimeFormat(timeSpec.getTimeFormat());
    datasetConfigDTO.setExpectedDelay(getExpectedDelayFromTimeunit(timeSpec.getTimeType()));
    if (timeSpec.getTimeFormat().startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString()) || timeSpec.getTimeFormat().equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
      datasetConfigDTO.setTimezone(PDT_TIMEZONE);
    }
    // set the data granularity of epoch timestamp dataset to minute-level
    if (datasetConfigDTO.getTimeFormat().equals(TimeSpec.SINCE_EPOCH_FORMAT) && datasetConfigDTO.getTimeUnit()
        .equals(TimeUnit.MILLISECONDS) && (datasetConfigDTO.getNonAdditiveBucketSize() == null
        || datasetConfigDTO.getNonAdditiveBucketUnit() == null)) {
      datasetConfigDTO.setNonAdditiveBucketUnit(TimeUnit.MINUTES);
      datasetConfigDTO.setNonAdditiveBucketSize(5);
    }
  }

  public static DatasetConfigDTO generateDatasetConfig(String dataset, Schema schema,
      Map<String, String> customConfigs) {
    List<String> dimensions = schema.getDimensionNames();
    TimeGranularitySpec timeSpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    // Create DatasetConfig
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(dataset);
    datasetConfigDTO.setDimensions(dimensions);
    setTimeSpecs(datasetConfigDTO, timeSpec);
    datasetConfigDTO.setDataSource(PinotThirdEyeDataSource.DATA_SOURCE_NAME);
    datasetConfigDTO.setProperties(customConfigs);
    checkNonAdditive(datasetConfigDTO);
    return datasetConfigDTO;
  }


  private static TimeGranularity getExpectedDelayFromTimeunit(TimeUnit timeUnit) {
    TimeGranularity expectedDelay = null;
    switch (timeUnit) {
      case HOURS:
      case MILLISECONDS:
      case MINUTES:
      case SECONDS:
        expectedDelay = DatasetConfigBean.DEFAULT_HOURLY_EXPECTED_DELAY;
        break;
      case DAYS:
      default:
        expectedDelay = DatasetConfigBean.DEFAULT_DAILY_EXPECTED_DELAY;
        break;
    }
    return expectedDelay;
  }

  /**
   * Check if the dataset is non-additive. If it is, set the additive flag to false and set the pre-aggregated keyword.
   * @param dataset the dataset DTO to check
   */
  static void checkNonAdditive(DatasetConfigDTO dataset) {
    if (dataset.isAdditive() && dataset.getDataset().endsWith(NON_ADDITIVE)) {
      dataset.setAdditive(false);
      dataset.setPreAggregatedKeyword(PINOT_PRE_AGGREGATED_KEYWORD);
    }
  }

  public static MetricConfigDTO generateMetricConfig(MetricFieldSpec metricFieldSpec, String dataset) {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    String metric = metricFieldSpec.getName();
    metricConfigDTO.setName(metric);
    metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(dataset, metric));
    metricConfigDTO.setDataset(dataset);

    String dataTypeStr = metricFieldSpec.getDataType().toString();
    if (BYTES_STRING.equals(dataTypeStr)) {
      // Assume if the column is BYTES type, use the default TDigest function and set the return data type to double
      metricConfigDTO.setDefaultAggFunction(MetricConfigBean.DEFAULT_TDIGEST_AGG_FUNCTION);
      metricConfigDTO.setDatatype(MetricType.DOUBLE);
    } else {
      metricConfigDTO.setDatatype(MetricType.valueOf(dataTypeStr));
    }

    return metricConfigDTO;
  }


  public static List<Long> getMetricIdsFromMetricConfigs(List<MetricConfigDTO> metricConfigs) {
    List<Long> metricIds = new ArrayList<>();
    for (MetricConfigDTO metricConfig : metricConfigs) {
      metricIds.add(metricConfig.getId());
    }
    return metricIds;
  }

}
