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
package org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue;

import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Mode for {@link ColumnMinMaxValueGenerator}.
 * <ul>
 *   <li>NONE: do not generate on any column</li>
 *   <li>TIME: generate on time column only</li>
 *   <li>NON_METRIC: generate on time/dimension columns</li>
 *   <li>ALL: generate on all columns</li>
 * </ul>
 */
public enum ColumnMinMaxValueGeneratorMode {
  NONE, TIME, NON_METRIC, ALL;

  public static final ColumnMinMaxValueGeneratorMode DEFAULT_MODE =
      ColumnMinMaxValueGeneratorMode.valueOf(CommonConstants.Server.DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE);
}
