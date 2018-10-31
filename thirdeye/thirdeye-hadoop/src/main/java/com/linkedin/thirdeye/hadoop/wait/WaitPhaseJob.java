/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.hadoop.wait;

import java.lang.reflect.Constructor;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.hadoop.wait.WaitPhaseJobConstants.*;

public class WaitPhaseJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(WaitPhaseJob.class);

  private String name;
  private Properties props;

  public WaitPhaseJob(String name, Properties props) {
    this.name = name;
    this.props = props;
  }

  public void run() {

    try {
      String thirdeyeWaitClass = getAndCheck(WAIT_UDF_CLASS.toString());

      if (thirdeyeWaitClass != null) {
        LOGGER.info("Initializing class {}", thirdeyeWaitClass);
        Constructor<?> constructor = Class.forName(thirdeyeWaitClass).getConstructor();
        WaitUDF waitUdf = (WaitUDF) constructor.newInstance();
        waitUdf.init(props);

        boolean complete = waitUdf.checkCompleteness();
        if (!complete) {
          throw new RuntimeException("Input folder {} has not received all records");
        }
      }
    }catch (Exception e) {
      LOGGER.error("Exception in waiting for inputs", e);
    }
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

}
