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

package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.DetectionConfigBean;
import com.linkedin.thirdeye.detection.spi.components.BaseComponent;
import java.util.HashMap;
import java.util.Map;


public class DetectionConfigDTO extends DetectionConfigBean {
  private Map<String, BaseComponent> components = new HashMap<>();

  public Map<String, BaseComponent> getComponents() {
    return components;
  }

  public void setComponents(Map<String, BaseComponent> components) {
    this.components = components;
  }
}
