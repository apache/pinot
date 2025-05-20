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
package org.apache.pinot.spi.data;

import java.util.Map;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class TimeBoundaryConfig extends BaseJsonConfig {
  private String _boundaryStrategy;
  private Map<String, Object> _parameters;

  public TimeBoundaryConfig() {
  }

  public TimeBoundaryConfig(String boundaryStrategy, Map<String, Object> parameters) {
    _boundaryStrategy = boundaryStrategy;
    _parameters = parameters;
  }

  public String getBoundaryStrategy() {
    return _boundaryStrategy;
  }

  public void setBoundaryStrategy(String boundaryStrategy) {
    _boundaryStrategy = boundaryStrategy;
  }

  public Map<String, Object> getParameters() {
    return _parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    _parameters = parameters;
  }
}
