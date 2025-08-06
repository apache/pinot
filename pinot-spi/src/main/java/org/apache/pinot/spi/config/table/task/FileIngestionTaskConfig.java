package org.apache.pinot.spi.config.table.task;

import java.util.Map;

public class FileIngestionTaskConfig extends TableTaskTypeConfig {

  FileIngestionTaskConfig(Map<String, String> configs) {
    super(configs);
  }

  @Override
  protected boolean validateConfig() {
    return false;
  }
}
