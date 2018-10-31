package com.linkedin.thirdeye.datasource.mock;

import com.linkedin.thirdeye.auto.onboard.AutoOnboard;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import com.linkedin.thirdeye.detection.ConfigUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scans mock configs and creates metrics for MockThirdEyeDataSource
 */
public class AutoOnboardMockDataSource extends AutoOnboard {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardMockDataSource.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;

  /**
   * Constructor for dependency injection
   *
   * @param metadataSourceConfig meta data source config
   */
  public AutoOnboardMockDataSource(MetadataSourceConfig metadataSourceConfig) {
    super(metadataSourceConfig);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
  }

  @Override
  public void run() {
    if (!this.datasetDAO.findAll().isEmpty()) {
      LOG.info("Found existing data set configs. Skipping.");
      return;
    }

    if (!this.metricDAO.findAll().isEmpty()) {
      LOG.info("Found existing metric configs. Skipping.");
      return;
    }

    MetadataSourceConfig config = this.getMetadataSourceConfig();

    List<DatasetConfigDTO> datasetConfigs = new ArrayList<>();
    List<MetricConfigDTO> metricConfigs = new ArrayList<>();

    Map<String, Object> datasets = (Map<String, Object>) config.getProperties().get("datasets");
    List<String> sortedDatasets = new ArrayList<>(datasets.keySet());
    Collections.sort(sortedDatasets);

    for (String datasetName : sortedDatasets) {
      Map<String, Object> dataset = (Map<String, Object>) datasets.get(datasetName);

      List<String> sortedDimensions = new ArrayList<>((Collection<String>) dataset.get("dimensions"));
      Collections.sort(sortedDimensions);

      Period granularity = ConfigUtils.parsePeriod(MapUtils.getString(dataset, "granularity", "1hour"));

      DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
      datasetConfig.setDataset(datasetName);
      datasetConfig.setDataSource(MockThirdEyeDataSource.class.getSimpleName());
      datasetConfig.setDimensions(sortedDimensions);
      datasetConfig.setTimezone(MapUtils.getString(dataset, "timezone", "America/Los_Angeles"));
      datasetConfig.setTimeDuration(getTimeDuration(granularity));
      datasetConfig.setTimeUnit(getTimeUnit(granularity));

      datasetConfigs.add(datasetConfig);

      List<String> sortedMetrics = new ArrayList<>(((Map<String, Object>) dataset.get("metrics")).keySet());
      Collections.sort(sortedMetrics);

      for (String metricName : sortedMetrics) {
        MetricConfigDTO metricConfig = new MetricConfigDTO();
        metricConfig.setName(metricName);
        metricConfig.setDataset(datasetName);
        metricConfig.setAlias(String.format("%s::%s", datasetName, metricName));

        metricConfigs.add(metricConfig);
      }
    }

    LOG.info("Read {} datasets and {} metrics", datasetConfigs.size(), metricConfigs.size());

    // NOTE: save in order as mock datasource expects metric ids first
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Long id = this.metricDAO.save(metricConfig);
      if (id != null) {
        LOG.info("Created metric '{}' with id {}", metricConfig.getAlias(), id);
      } else {
        LOG.warn("Could not create metric '{}'", metricConfig.getAlias());
      }
    }

    for (DatasetConfigDTO datasetConfig : datasetConfigs) {
      Long id = this.datasetDAO.save(datasetConfig);
      if (id != null) {
        LOG.info("Created dataset '{}' with id {}", datasetConfig.getDataset(), id);
      } else {
        LOG.warn("Could not create dataset '{}'", datasetConfig.getDataset());
      }
    }
  }

  @Override
  public void runAdhoc() {
    this.run();
  }

  /**
   * Guess time duration from period.
   *
   * @param granularity dataset granularity
   * @return
   */
  private static int getTimeDuration(Period granularity) {
    if (granularity.getDays() > 0) {
      return granularity.getDays();
    }
    if (granularity.getHours() > 0) {
      return granularity.getHours();
    }
    if (granularity.getMinutes() > 0) {
      return granularity.getMinutes();
    }
    if (granularity.getSeconds() > 0) {
      return granularity.getSeconds();
    }
    return granularity.getMillis();
  }

  /**
   * Guess time unit from period.
   *
   * @param granularity dataset granularity
   * @return
   */
  private static TimeUnit getTimeUnit(Period granularity) {
    if (granularity.getDays() > 0) {
      return TimeUnit.DAYS;
    }
    if (granularity.getHours() > 0) {
      return TimeUnit.HOURS;
    }
    if (granularity.getMinutes() > 0) {
      return TimeUnit.MINUTES;
    }
    if (granularity.getSeconds() > 0) {
      return TimeUnit.SECONDS;
    }
    return TimeUnit.MILLISECONDS;
  }
}
