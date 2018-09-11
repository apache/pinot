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

package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import java.lang.reflect.Constructor;


/**
 * The Detection alert filter loader.
 */
public class DetectionAlertFilterLoader {
  private static final String PROP_CLASS_NAME = "className";

  /**
   * Return a detection alert filter from detection alert filter.
   *
   * @param provider the provider
   * @param config the config
   * @param endTime the end time stamp
   * @return the detection alert filter
   * @throws Exception the exception
   */
  public DetectionAlertFilter from(DataProvider provider, DetectionAlertConfigDTO config, long endTime)
      throws Exception {
    String className = config.getProperties().get(PROP_CLASS_NAME).toString();
    Constructor<?> constructor = Class.forName(className)
        .getConstructor(DataProvider.class, DetectionAlertConfigDTO.class, long.class);
    return (DetectionAlertFilter) constructor.newInstance(provider, config, endTime);
  }
}
