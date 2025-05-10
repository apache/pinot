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
package org.apache.pinot.spi.utils;

import java.util.function.BooleanSupplier;


/**
 * This enum is used to represent the enablement status of a feature.
 * It can be used to enable, disable, or use the default instance level enablement of a feature.
 */
public enum Enablement {
  ENABLE,   // Enable a feature
  DISABLE,  // Disable a feature
  DEFAULT;  // Use the default enablement of the feature

  public boolean isEnabled(boolean defaultValue) {
    if (this == ENABLE) {
      return true;
    }
    if (this == DISABLE) {
      return false;
    }
    return defaultValue;
  }

  public boolean isEnabled(BooleanSupplier defaultValueSupplier) {
    if (this == ENABLE) {
      return true;
    }
    if (this == DISABLE) {
      return false;
    }
    return defaultValueSupplier.getAsBoolean();
  }
}
