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
package org.apache.pinot.core.operator.filter;

import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.FilterBlock;


/**
 * The {@link BaseFilterOperator} class is the base class for all filter operators.
 */
public abstract class BaseFilterOperator extends BaseOperator<FilterBlock> {

  /**
   * Returns {@code true} if the result is always empty, {@code false} otherwise.
   */
  public boolean isResultEmpty() {
    return false;
  }

  /**
   * Returns {@code true} if the result matches all the records, {@code false} otherwise.
   */
  public boolean isResultMatchingAll() {
    return false;
  }
}
