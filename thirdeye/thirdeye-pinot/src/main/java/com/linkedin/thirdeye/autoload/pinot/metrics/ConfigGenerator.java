package com.linkedin.thirdeye.autoload.pinot.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class ConfigGenerator {

  private static final String PDT_TIMEZONE = "US/Pacific";

  public static DatasetConfigDTO generateDatasetConfig(String dataset, Schema schema) {
    List<String> dimensions = schema.getDimensionNames();
    TimeGranularitySpec timeSpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();

    // Create DatasetConfig
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(dataset);
    datasetConfigDTO.setDimensions(dimensions);
    datasetConfigDTO.setTimeColumn(timeSpec.getName());
    datasetConfigDTO.setTimeDuration(timeSpec.getTimeUnitSize());
    datasetConfigDTO.setTimeUnit(timeSpec.getTimeType());
    datasetConfigDTO.setTimeFormat(timeSpec.getTimeFormat());
    datasetConfigDTO.setExpectedDelay(getExpectedDelayFromTimeunit(datasetConfigDTO.getTimeUnit()));
    if (timeSpec.getTimeFormat().startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
      datasetConfigDTO.setTimezone(PDT_TIMEZONE);
    }
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
    metricConfigDTO.setDatatype(MetricType.valueOf(metricFieldSpec.getDataType().toString()));
    return metricConfigDTO;
  }


  public static DashboardConfigDTO generateDefaultDashboardConfig(String dataset, List<Long> metricIds) {
    DashboardConfigDTO dashboardConfigDTO = new DashboardConfigDTO();
    String dashboardName = DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset;
    dashboardConfigDTO.setName(dashboardName);
    dashboardConfigDTO.setDataset(dataset);
    dashboardConfigDTO.setMetricIds(metricIds);
    return dashboardConfigDTO;
  }

  public static List<Long> getMetricIdsFromMetricConfigs(List<MetricConfigDTO> metricConfigs) {
    List<Long> metricIds = new ArrayList<>();
    for (MetricConfigDTO metricConfig : metricConfigs) {
      metricIds.add(metricConfig.getId());
    }
    return metricIds;
  }



}
