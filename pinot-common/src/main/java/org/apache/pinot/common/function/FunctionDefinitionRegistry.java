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
package org.apache.pinot.common.function;

import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Place where all functions are registered.
 */
public class FunctionDefinitionRegistry {
  private FunctionDefinitionRegistry() {
  }

  public static boolean isAggFunc(String functionName) {
    try {
      AggregationFunctionType.getAggregationFunctionType(functionName);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static boolean isTransformFunc(String functionName) {
    try {
      TransformFunctionType.getTransformFunctionType(functionName);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
