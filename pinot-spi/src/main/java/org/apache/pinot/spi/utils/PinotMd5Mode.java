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

/**
 * Global runtime switch for disabling MD5-dependent code paths in Pinot.
 */
public class PinotMd5Mode {
  private static volatile boolean _pinotMd5Disabled = readFromSystemProperty();

  private PinotMd5Mode() {
  }

  public static void setPinotMd5Disabled(boolean pinotMd5Disabled) {
    _pinotMd5Disabled = pinotMd5Disabled;
  }

  public static boolean isPinotMd5Disabled() {
    return _pinotMd5Disabled;
  }

  // Visible for tests in the same package.
  static void resetFromSystemProperty() {
    _pinotMd5Disabled = readFromSystemProperty();
  }

  private static boolean readFromSystemProperty() {
    return Boolean.getBoolean(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED);
  }
}
