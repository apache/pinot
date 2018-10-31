package com.linkedin.thirdeye.auto.onboard;

import com.google.common.base.CaseFormat;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.DataSources;
import com.linkedin.thirdeye.datasource.DataSourcesLoader;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutoOnboardUtility {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardUtility.class);

  private static final String DEFAULT_ALERT_GROUP_PREFIX = "auto_onboard_dataset_";
  private static final String DEFAULT_ALERT_GROUP_SUFFIX = "_alert";

  public static Map<String, List<AutoOnboard>> getDataSourceToAutoOnboardMap(URL dataSourcesUrl) {
    Map<String, List<AutoOnboard>> dataSourceToOnboardMap = new HashMap<>();

    DataSources dataSources = DataSourcesLoader.fromDataSourcesUrl(dataSourcesUrl);
    if (dataSources == null) {
      throw new IllegalStateException("Could not create data sources config from path " + dataSourcesUrl);
    }
    for (DataSourceConfig dataSourceConfig : dataSources.getDataSourceConfigs()) {
      List<MetadataSourceConfig> metadataSourceConfigs = dataSourceConfig.getMetadataSourceConfigs();
      if (metadataSourceConfigs != null) {
        for (MetadataSourceConfig metadataSourceConfig : metadataSourceConfigs) {
          String metadataSourceClassName = metadataSourceConfig.getClassName();
          // Inherit properties from Data Source
          metadataSourceConfig.getProperties().putAll(dataSourceConfig.getProperties());
          if (StringUtils.isNotBlank(metadataSourceClassName)) {
            try {
              Constructor<?> constructor = Class.forName(metadataSourceClassName).getConstructor(MetadataSourceConfig.class);
              AutoOnboard autoOnboardConstructor = (AutoOnboard) constructor.newInstance(metadataSourceConfig);
              String datasourceClassName = dataSourceConfig.getClassName();
              String dataSource =
                  datasourceClassName.substring(datasourceClassName.lastIndexOf(".") + 1, datasourceClassName.length());

              if (dataSourceToOnboardMap.containsKey(dataSource)) {
                dataSourceToOnboardMap.get(dataSource).add(autoOnboardConstructor);
              } else {
                List<AutoOnboard> autoOnboardServices = new ArrayList<>();
                autoOnboardServices.add(autoOnboardConstructor);
                dataSourceToOnboardMap.put(dataSource, autoOnboardServices);
              }
            } catch (Exception e) {
              LOG.error("Exception in creating metadata constructor {}", metadataSourceClassName, e);
            }
          }
        }
      }
    }

    return dataSourceToOnboardMap;
  }

  public static String getAutoAlertGroupName(String dataset) {
    return DEFAULT_ALERT_GROUP_PREFIX
        + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, dataset) + DEFAULT_ALERT_GROUP_SUFFIX;
  }
}
