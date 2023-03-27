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
package org.apache.pinot.segment.spi.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


public class JavaVersion {

  public static final int VERSION;

  static {
    int version;
    try {
      Method versionMethod = Runtime.class.getMethod("version");
      Object versionObject = versionMethod.invoke(null);
      try {
        Method featureMethod = versionObject.getClass().getMethod("feature");
        Object featureVersionValue = featureMethod.invoke(versionObject);
        version = (int) featureVersionValue;
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        // If Runtime.version() can be called but Version.feature() cannot be called,
        // we have to be in Java = 9
        version = 9;
      }
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      // If Runtime.version() cannot be called, we have to be in Java <= 9
      // it could be an older version, but Pinot only supports Java >= 8
      version = 8;
    }
    VERSION = version;
  }

  private JavaVersion() {
  }
}
