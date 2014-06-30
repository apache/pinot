package com.linkedin.pinot.query.config;

import java.util.ArrayList;
import java.util.Iterator;
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
    Iterator keysIterator = _segmentPrunerSetConfig.getKeys();
    while (keysIterator.hasNext()) {
      String key = (String) keysIterator.next();
      if (key.endsWith(".class")) {
        String prefix = key.substring(0, key.indexOf(".class"));
        String serviceClass = _segmentPrunerSetConfig.getString(key);
        _segmentPrunerClassNameList.add(serviceClass);
        _segmentPrunerConfigurationList.add(_segmentPrunerSetConfig.subset(prefix));
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
