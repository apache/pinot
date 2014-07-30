package com.linkedin.pinot.query.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;


public class SimpleMetadataPersistentManager {

  public static Map<String, SimpleColumnMetadata> readFromConfig(Configuration config) {
    HashMap<String, SimpleColumnMetadata> columnMetadataMap = new HashMap<String, SimpleColumnMetadata>();
    Iterator columns = config.getKeys("column");
    while (columns.hasNext()) {
      String key = (String) columns.next();
      String columnName = key.split("\\.")[1];
      SimpleColumnMetadata metadata = SimpleColumnMetadata.valueOf(config, columnName);
      if (!columnMetadataMap.containsKey(columnName)) {
        columnMetadataMap.put(columnName, metadata);
      }

    }
    return columnMetadataMap;
  }

}
