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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * This class represents the configuration for a physical table in {@link LogicalTableConfig}.
 * This is empty by design and more docs would be added as features are added.
 */
public class PhysicalTableConfig extends BaseJsonConfig {
  @JsonProperty("isFederated")
  boolean _isFederated;

  /**
   * Default constructor that creates a non-federated physical table config.
   */
  public PhysicalTableConfig() {
    this(false);
  }

  /**
   * Constructor that creates a physical table config with the specified federation status.
   *
   * @param isFederated true if this physical table is federated across clusters, false otherwise
   */
  public PhysicalTableConfig(boolean isFederated) {
    _isFederated = isFederated;
  }

  /**
   * Returns whether this physical table is federated across clusters.
   *
   * @return true if the physical table is federated, false otherwise
   */
  public boolean isFederated() {
    return _isFederated;
  }

  /**
   * Sets whether this physical table is federated across clusters.
   *
   * @param isFederated true if the physical table is federated, false otherwise
   */
  public void setFederated(boolean isFederated) {
    _isFederated = isFederated;
  }
}
