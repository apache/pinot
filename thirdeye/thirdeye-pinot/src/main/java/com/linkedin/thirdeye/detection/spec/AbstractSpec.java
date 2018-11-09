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

package com.linkedin.thirdeye.detection.spec;

import java.util.Map;
import org.modelmapper.ModelMapper;


/**
 * Base class for component specs
 */
public abstract class AbstractSpec {
  protected static final ModelMapper MODEL_MAPPER = new ModelMapper();

  public static <T extends AbstractSpec> T fromProperties(Map<String, Object> properties, Class<T> specClass) {
    T spec = MODEL_MAPPER.map(properties, specClass);
    return spec;
  }
}
