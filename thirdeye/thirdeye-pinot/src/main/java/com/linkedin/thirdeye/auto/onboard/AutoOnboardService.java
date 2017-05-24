package com.linkedin.thirdeye.auto.onboard;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.DataSources;
import com.linkedin.thirdeye.datasource.DataSourcesLoader;

/**
 * This is a service to onboard datasets automatically to thirdeye from the different data sources
 * This service runs periodically and runs auto load for each data source
 */
public class AutoOnboardService implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardService.class);


  private ScheduledExecutorService scheduledExecutorService;
  private DataSources dataSources;
  private List<AutoOnboard> autoOnboardServices = new ArrayList<>();
  private TimeGranularity runFrequency;

  /**
   * Reads data sources configs and instantiates the constructors for auto load of all data sources, if availble
   * @param config
   */
  public AutoOnboardService(ThirdEyeAnomalyConfiguration config) {
    this.runFrequency = config.getAutoOnboardConfiguration().getRunFrequency();
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    String dataSourcesPath = config.getDataSourcesPath();
    dataSources = DataSourcesLoader.fromDataSourcesPath(dataSourcesPath);
    if (dataSources == null) {
      throw new IllegalStateException("Could not create data sources config from path " + dataSourcesPath);
    }
    for (DataSourceConfig dataSourceConfig : dataSources.getDataSourceConfigs()) {
      String autoLoadClassName = dataSourceConfig.getAutoLoadClassName();
      if (StringUtils.isNotBlank(autoLoadClassName)) {
        try {
          Constructor<?> constructor = Class.forName(autoLoadClassName).getConstructor(DataSourceConfig.class);
          AutoOnboard autoOnboardConstructor = (AutoOnboard) constructor.newInstance(dataSourceConfig);
          autoOnboardServices.add(autoOnboardConstructor);
        } catch (Exception e) {
          LOG.error("Exception in creating autoload constructor {}", autoLoadClassName);
        }
      }
    }
  }

  public void start() {
    scheduledExecutorService.scheduleAtFixedRate(this, 0, runFrequency.getSize(), runFrequency.getUnit());
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  public void run() {
    for (AutoOnboard autoOnboard : autoOnboardServices) {
      autoOnboard.run();
    }
  }

}
