package com.linkedin.pinot.core.query.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Config for SegmentPruner.
 * 
 * @author xiafu
 *
 */
public class SegmentPrunerConfig {

  // key of segment pruner classes
  private static String SEGMENT_PRUNER_CLASS = "class";

  private Configuration _segmentPrunerSetConfig;
  private static String[] REQUIRED_KEYS = {};

  private List<String> _segmentPrunerClassNameList = new ArrayList<String>();
  private List<Configuration> _segmentPrunerConfigurationList = new ArrayList<Configuration>();

  public SegmentPrunerConfig(Configuration segmentPrunerConfig) throws ConfigurationException {
    _segmentPrunerSetConfig = segmentPrunerConfig;
    checkRequiredKeys();
    String[] serviceClasses = _segmentPrunerSetConfig.getStringArray(SEGMENT_PRUNER_CLASS);

    for (String serviceClass : serviceClasses) {
      _segmentPrunerClassNameList.add(serviceClass);
      _segmentPrunerConfigurationList.add(_segmentPrunerSetConfig.subset(serviceClass));
    }
  }

  private void checkRequiredKeys() throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_segmentPrunerSetConfig.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  public String getSegmentPrunerName(int index) {
    return _segmentPrunerClassNameList.get(index);
  }

  public Configuration getSegmentPrunerConfig(int index) {
    return _segmentPrunerConfigurationList.get(index);
  }

  public int numberOfSegmentPruner() {
    return _segmentPrunerConfigurationList.size();
  }

}
