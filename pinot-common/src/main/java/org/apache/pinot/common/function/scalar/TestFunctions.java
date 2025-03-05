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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * A number  of functions used for testing, e.g. crashes during query compilation or processing.
 * They're not meant for production use.
 */
public class TestFunctions {

  private static volatile boolean _enabled = false;

  public static void enable() {
    _enabled = true;
  }

  public static void disable() {
    _enabled = false;
  }

  /*
   * If 'ignored' is a column reference, calcite shouldn't evaluate expression at compile time
   * and replace with a constant.
   * */
  @ScalarFunction
  public long throwError(int ignored) {
    if (_enabled) {
      throw new RuntimeException("The sky is falling!");
    }

    return 0L;
  }
}
