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
package org.apache.pinot.segment.local.upsert.merger;

import org.apache.pinot.spi.data.readers.GenericRow;


public class IncrementMerger implements PartialUpsertMerger {

  private final String _fieldName;

  public IncrementMerger(String fieldName) {
    _fieldName = fieldName;
  }

  /**
   * Increment the new value from incoming row to the given field of previous record.
   */
  public GenericRow merge(GenericRow previousRecord, GenericRow currentRecord) {

    assert previousRecord.getValue(_fieldName) instanceof Number;
    assert currentRecord.getValue(_fieldName) instanceof Number;
    previousRecord.putValue(_fieldName,
        addNumbers((Number) previousRecord.getValue(_fieldName), (Number) currentRecord.getValue(_fieldName)));
    return previousRecord;
  }

  private static Number addNumbers(Number a, Number b) {
    if (a instanceof Double || b instanceof Double) {
      return a.doubleValue() + b.doubleValue();
    } else if (a instanceof Float || b instanceof Float) {
      return a.floatValue() + b.floatValue();
    } else if (a instanceof Long || b instanceof Long) {
      return a.longValue() + b.longValue();
    }

    return a.intValue() + b.intValue();
  }
}
