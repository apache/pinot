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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.segment.local.utils.SpecialValueTransformerUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;


/**
 * The {@code SpecialValueTransformer} class will transform special values according to the following rules:
 * <ul>
 *   <li>
 *     For FLOAT and DOUBLE:
 *     <ul>
 *       <li>Negative zero (-0.0) should be converted to 0.0</li>
 *       <li>NaN should be converted to default null</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For BIG_DECIMAL:
 *     <ul>
 *       <li>Strip trailing zeros</li>
 *     </ul>
 *   </li>
 * </ul>
 * <p>This transformation is required to ensure that the value is equal to itself, and the ordering of the values is
 * consistent with equals. This is required for certain data structures (e.g. sorted map) and algorithm (e.g. binary
 * search) to work correctly. Read more about it in {@link Comparable}.
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that we already have the values complying
 * with the schema before handling special values and before {@link NullValueTransformer} so that it transforms
 * all the null values properly.
 */
public class SpecialValueTransformer implements RecordTransformer {

  private final Set<String> _columnsToCheck = new HashSet<>();

  public SpecialValueTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && SpecialValueTransformerUtils.needsTransformation(fieldSpec)) {
        _columnsToCheck.add(fieldSpec.getName());
      }
    }
  }

  @Override
  public boolean isNoOp() {
    return _columnsToCheck.isEmpty();
  }

  @Override
  public void transform(GenericRow record) {
    for (String column : _columnsToCheck) {
      Object value = record.getValue(column);
      if (value instanceof Object[]) {
        // Multi-valued column.
        Object[] transformedValues = SpecialValueTransformerUtils.transformValues((Object[]) value);
        if (transformedValues != value) {
          record.putValue(column, transformedValues);
        }
      } else if (value != null) {
        // Single-valued column.
        Object transformedValue = SpecialValueTransformerUtils.transformValue(value);
        if (transformedValue != value) {
          record.putValue(column, transformedValue);
        }
      }
    }
  }
}
