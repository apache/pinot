package com.linkedin.pinot.core.data.manager.config;

import org.apache.commons.configuration.Configuration;
import com.linkedin.pinot.common.segment.ReadMode;


public interface InstanceDataManagerConfig {
  Configuration getConfig();

  String getInstanceId();

  String getInstanceDataDir();

  String getInstanceSegmentTarDir();

  String getInstanceBootstrapSegmentDir();

  String getSegmentMetadataLoaderClass();

  ReadMode getReadMode();
}
