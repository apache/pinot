/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Config for SegmentPruner.
 *
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
