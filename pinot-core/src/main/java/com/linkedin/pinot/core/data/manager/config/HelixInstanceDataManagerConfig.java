package com.linkedin.pinot.core.data.manager.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.common.segment.ReadMode;


/**
 * The config used for HelixInstanceDataManager.
 * 
 * @author xiafu
 *
 */
public class HelixInstanceDataManagerConfig {

  private static final String INSTANCE_SEGMENT_METADATA_LOADER_CLASS = "segment.metadata.loader.class";
  // Key of instance id
  public static final String INSTANCE_ID = "id";
  // Key of instance data directory
  public static final String INSTANCE_DATA_DIR = "dataDir";
  // Key of instance segment tar directory
  public static final String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
  // Key of segment directory
  public static final String INSTANCE_BOOTSTRAP_SEGMENT_DIR = "bootstrap.segment.dir";
  // Key of resource data directory
  public static final String kEY_OF_RESOURCE_DATA_DIRECTORY = "directory";
  // Key of resource data directory
  public static final String kEY_OF_RESOURCE_NAME = "name";
  // Key of instance level segment read mode.
  public static final String READ_MODE = "readMode";

  private final static String[] REQUIRED_KEYS = { INSTANCE_ID, INSTANCE_DATA_DIR, READ_MODE };
  private Configuration _instanceDataManagerConfiguration = null;

  public HelixInstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;
    checkRequiredKeys();
  }

  private void checkRequiredKeys() throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_instanceDataManagerConfiguration.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  public Configuration getConfig() {
    return _instanceDataManagerConfiguration;
  }

  public String getInstanceId() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_ID);
  }

  public String getInstanceDataDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_DATA_DIR);
  }

  public String getInstanceBootstrapSegmentDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_BOOTSTRAP_SEGMENT_DIR);
  }

  public String getSegmentMetadataLoaderClass() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_METADATA_LOADER_CLASS);
  }

  public ReadMode getReadMode() {
    return ReadMode.valueOf(_instanceDataManagerConfiguration.getString(READ_MODE));
  }

  public String getInstanceSegmentTarDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_TAR_DIR);
  }
}
