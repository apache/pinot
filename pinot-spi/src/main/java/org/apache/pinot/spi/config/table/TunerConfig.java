/**
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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;

import static org.apache.pinot.spi.config.table.TableConfig.TUNER_CONFIG;


/**
 * Encapsulates custom config for {@link org.apache.pinot.spi.config.table.tuner.TableConfigTuner}
 * The specified properties must have a 'name' field which specifies the exact type of tuner used
 * for this table.
 */
public class TunerConfig extends BaseJsonConfig {
  private static final String NAME = "name";
  private final Map<String, String> _tunerProperties;

  @JsonCreator
  public TunerConfig(@JsonProperty(TUNER_CONFIG) @Nullable Map<String, String> tunerProperties) {
    _tunerProperties = tunerProperties;
  }

  public String name() {
    return _tunerProperties.get(NAME);
  }

  @Nullable
  public Map<String, String> getTunerProperties() {
    return _tunerProperties;
  }
}
