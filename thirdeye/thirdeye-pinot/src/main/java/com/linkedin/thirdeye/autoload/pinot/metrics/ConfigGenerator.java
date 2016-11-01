package com.linkedin.thirdeye.autoload.pinot.metrics;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class ConfigGenerator {

  private static final String PDT_TIMEZONE = "US/Pacific";
  private static final String DEFAULT_INGRAPH_METRIC_NAMES_COLUMN = "metricAlias";
  private static final String DEFAULT_INGRAPH_METRIC_VALUES_COLUMN = "value";

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
    if (timeSpec.getTimeFormat().startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
      datasetConfigDTO.setTimezone(PDT_TIMEZONE);
    }
    return datasetConfigDTO;
  }

  public static DatasetConfigDTO generateIngraphDatasetConfig(String dataset, Schema schema) {
    DatasetConfigDTO datasetConfigDTO = generateDatasetConfig(dataset, schema);
    datasetConfigDTO.setMetricAsDimension(true);
    datasetConfigDTO.setMetricNamesColumn(DEFAULT_INGRAPH_METRIC_NAMES_COLUMN);
    datasetConfigDTO.setMetricValuesColumn(DEFAULT_INGRAPH_METRIC_VALUES_COLUMN);

    return datasetConfigDTO;
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

  public static MetricConfigDTO generateIngraphMetricConfig(IngraphMetricConfigDTO ingraphMetricConfig) {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName(ingraphMetricConfig.getMetricAlias());
    metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(
        ingraphMetricConfig.getDataset(), ingraphMetricConfig.getMetricAlias()));
    metricConfigDTO.setDataset(ingraphMetricConfig.getDataset());
    metricConfigDTO.setDatatype(MetricType.valueOf(ingraphMetricConfig.getMetricDataType()));
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


  public static boolean isIngraphDataset(Schema schema) {
    boolean isIngraphDataset = false;
    if (schema.getDimensionNames().contains(DEFAULT_INGRAPH_METRIC_NAMES_COLUMN)
        && schema.getMetricNames().contains(DEFAULT_INGRAPH_METRIC_VALUES_COLUMN)) {
      isIngraphDataset = true;
    }
    return isIngraphDataset;
  }

}
