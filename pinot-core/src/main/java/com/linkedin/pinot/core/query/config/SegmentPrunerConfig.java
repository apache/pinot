package com.linkedin.pinot.core.query.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;


/**
 * Config for SegmentPruner.
 * 
 * @author xiafu
 *
 */
public class SegmentPrunerConfig {

  private Configuration _segmentPrunerSetConfig;
  private List<String> _segmentPrunerClassNameList = new ArrayList<String>();
  private List<Configuration> _segmentPrunerConfigurationList = new ArrayList<Configuration>();

  public SegmentPrunerConfig(Configuration segmentPrunerConfig) {
    _segmentPrunerSetConfig = segmentPrunerConfig;
    String[] serviceClasses = _segmentPrunerSetConfig.getStringArray("class");

    for (String serviceClass : serviceClasses) {
      _segmentPrunerClassNameList.add(serviceClass);
      _segmentPrunerConfigurationList.add(_segmentPrunerSetConfig.subset(serviceClass));
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
