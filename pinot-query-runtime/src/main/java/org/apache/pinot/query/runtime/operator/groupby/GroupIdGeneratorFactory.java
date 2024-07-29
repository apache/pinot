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
package org.apache.pinot.query.runtime.operator.groupby;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


public class GroupIdGeneratorFactory {
  private GroupIdGeneratorFactory() {
  }

  public static GroupIdGenerator getGroupIdGenerator(ColumnDataType[] keyTypes, int numKeyColumns, int numGroupsLimit) {
    if (numKeyColumns == 1) {
      switch (keyTypes[0]) {
        case INT:
          return new OneIntKeyGroupIdGenerator(numGroupsLimit);
        case LONG:
          return new OneLongKeyGroupIdGenerator(numGroupsLimit);
        case FLOAT:
          return new OneFloatKeyGroupIdGenerator(numGroupsLimit);
        case DOUBLE:
          return new OneDoubleKeyGroupIdGenerator(numGroupsLimit);
        default:
          return new OneObjectKeyGroupIdGenerator(numGroupsLimit);
      }
    } else if (numKeyColumns == 2) {
      return new TwoKeysGroupIdGenerator(keyTypes[0], keyTypes[1], numGroupsLimit);
    } else {
      return new MultiKeysGroupIdGenerator(keyTypes, numKeyColumns, numGroupsLimit);
    }
  }
}
