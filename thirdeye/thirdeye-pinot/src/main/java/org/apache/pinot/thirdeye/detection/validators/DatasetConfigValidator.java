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
 */

package org.apache.pinot.thirdeye.detection.validators;

import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;

public class DatasetConfigValidator implements ConfigValidator<DatasetConfigDTO> {
  private static final String DEFAULT_DATASET_NAME = "online_dataset";

  @Override
  public void validateConfig(DatasetConfigDTO config) throws IllegalArgumentException {
    Preconditions.checkArgument(config.getName().startsWith(DEFAULT_DATASET_NAME));
  }

  @Override
  public void validateYaml(Map<String, Object> config) throws IllegalArgumentException {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void validateUpdatedConfig(DatasetConfigDTO updatedConfig, DatasetConfigDTO oldConfig) throws IllegalArgumentException {
    throw new RuntimeException("Not supported");
  }
}
