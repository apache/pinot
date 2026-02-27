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

import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for array scalar functions in ArrayFunctions class.
 * Tests both existing functions and newly added Trino-compatible functions.
 */
public class ArrayFunctionsTest {

  // Test data
  private static final int[] INT_ARRAY = {1, 2, 3, 4, 5};
  private static final int[] EMPTY_INT_ARRAY = {};
  private static final long[] LONG_ARRAY = {10L, 20L, 30L, 40L, 50L};
  private static final float[] FLOAT_ARRAY = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
  private static final double[] DOUBLE_ARRAY = {1.1, 2.2, 3.3, 4.4, 5.5};
  private static final String[] STRING_ARRAY = {"apple", "banana", "cherry", "date", "elderberry"};
  private static final String[] EMPTY_STRING_ARRAY = {};

}
