/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.pinot.thirdeye.detection.spec;

import java.util.Map;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;


/**
 * Base class for component specs
 */
public abstract class AbstractSpec {
  static final String DEFAULT_TIMEZONE = "America/Los_Angeles";

  public static <T extends AbstractSpec> T fromProperties(Map<String, Object> properties, Class<T> specClass) {
    // don't reuse model mapper instance. It caches typeMaps and will result in unexpected mappings
    ModelMapper modelMapper = new ModelMapper();
    // use strict mapping to ensure no mismatches or ambiguity occurs
    modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
    return modelMapper.map(properties, specClass);
  }
}
