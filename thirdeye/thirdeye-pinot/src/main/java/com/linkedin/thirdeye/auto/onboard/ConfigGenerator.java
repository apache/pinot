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

package com.linkedin.thirdeye.auto.onboard;

import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class ConfigGenerator {

  private static final String PDT_TIMEZONE = "US/Pacific";
  private static final String BYTES_STRING = "BYTES";

  public static void setTimeSpecs(DatasetConfigDTO datasetConfigDTO, TimeGranularitySpec timeSpec) {
    datasetConfigDTO.setTimeColumn(timeSpec.getName());
    datasetConfigDTO.setTimeDuration(timeSpec.getTimeUnitSize());
    datasetConfigDTO.setTimeUnit(timeSpec.getTimeType());
    datasetConfigDTO.setTimeFormat(timeSpec.getTimeFormat());
    datasetConfigDTO.setExpectedDelay(getExpectedDelayFromTimeunit(timeSpec.getTimeType()));
    if (timeSpec.getTimeFormat().startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
      datasetConfigDTO.setTimezone(PDT_TIMEZONE);
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
