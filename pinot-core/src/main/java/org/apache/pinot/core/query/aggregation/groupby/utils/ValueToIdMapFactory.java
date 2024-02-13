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
package org.apache.pinot.core.query.aggregation.groupby.utils;

import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory for various implementations for {@link ValueToIdMap}
 */
public class ValueToIdMapFactory {
  private ValueToIdMapFactory() {
  }

  public static ValueToIdMap get(DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntToIdMap();
      case LONG:
        return new LongToIdMap();
      case FLOAT:
        return new FloatToIdMap();
      case DOUBLE:
        return new DoubleToIdMap();
      default:
        return new ObjectToIdMap();
    }
  }
}
