/*
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

package com.linkedin.thirdeye.detection.tune;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TunerLoader {
  private static final Logger LOG = LoggerFactory.getLogger(TunerLoader.class);
  private static final String PROP_CLASS_NAME = "className";

  public PipelineTuner from(DetectionConfigDTO config, long startTime, long endTime) {
    PipelineTuner tuner;
    try {
      String className = config.getTunerProperties().get(PROP_CLASS_NAME).toString();
      tuner = (PipelineTuner) Class.forName(className).newInstance();
    } catch (Exception e) {
      LOG.warn("Loading tuner for detection config {} failed, use dummy tuner", config.getId(), e);
      tuner = new DummyTuner();
    }
    tuner.init(config.getTunerProperties(), config.getId(), startTime, endTime);
    return tuner;
  }
}
