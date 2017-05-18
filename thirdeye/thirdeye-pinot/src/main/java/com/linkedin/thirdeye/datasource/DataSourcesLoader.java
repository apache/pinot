package com.linkedin.thirdeye.datasource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * This class contains helper classes to load/transform datasources from file
 */
public class DataSourcesLoader {
 private static final Logger LOG = LoggerFactory.getLogger(DataSourcesLoader.class);
 private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

 /**
  * Returns datasources config from yml file
  * @param dataSourceConfigPath
  * @return DataSources
  */
 public static DataSources fromDataSourcesPath(String dataSourcesPath) {
   DataSources dataSources = null;
   try {
     dataSources = OBJECT_MAPPER.readValue(new File(dataSourcesPath), DataSources.class);
   } catch (IOException e) {
     LOG.error("Exception in reading data sources file {}", dataSourcesPath, e);
   }
   return dataSources;
 }

 /**
  * Returns datasource name to datasource map
  * @param dataSources
  * @return
  */
 public static Map<String, ThirdEyeDataSource> getDataSourceMap(DataSources dataSources) {
   Map<String, ThirdEyeDataSource> dataSourceMap = new HashMap<>();
   for (DataSourceConfig dataSourceConfig : dataSources.getDataSourceConfigs()) {
     String className = dataSourceConfig.getClassName();
     Map<String, String> properties = dataSourceConfig.getProperties();
     try {
       LOG.info("Creating thirdeye datasource {} with properties '{}'", className, properties);
       Constructor<?> constructor = Class.forName(className).getConstructor(Map.class);
       ThirdEyeDataSource thirdeyeDataSource = (ThirdEyeDataSource) constructor.newInstance(properties);
       // use class simple name as key, this enforces that there cannot be more than one data source of the same type
       String name = thirdeyeDataSource.getName();
       if (dataSourceMap.containsKey(name)) {
         throw new IllegalStateException("Data source " + name + " already exists. "
             + "There can be only ONE datasource of each type");
       }
       dataSourceMap.put(name, thirdeyeDataSource);
     } catch (Exception e) {
       LOG.error("Exception in creating thirdeye data source {}", className, e);
     }
   }
   return dataSourceMap;
 }

}
