/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;


public class AvroRecordToPinotRowGenerator {
  private final Schema _schema;
  private final FieldSpec _incomingTimeFieldSpec;

  public AvroRecordToPinotRowGenerator(@Nonnull Schema schema) {
    _schema = schema;

    // For time field, we use the incoming time field spec
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    Preconditions.checkNotNull(timeFieldSpec);
    _incomingTimeFieldSpec = new TimeFieldSpec(timeFieldSpec.getIncomingGranularitySpec());
  }

  @Nonnull
  public GenericRow transform(@Nonnull GenericData.Record avroRecord, @Nonnull GenericRow destination) {
    FieldSpec incomingFieldSpec;
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      switch (fieldSpec.getFieldType()) {
        case TIME:
          incomingFieldSpec = _incomingTimeFieldSpec;
          break;
        default:
          incomingFieldSpec = fieldSpec;
          break;
      }
      String columnName = incomingFieldSpec.getName();

      Object entry = avroRecord.get(columnName);
      if (entry != null) {
        // Entry is not null
        if (entry instanceof Array) {
          Object[] entryArray = AvroRecordReader.transformAvroArrayToObjectArray((Array) entry, incomingFieldSpec);
          if (incomingFieldSpec.getDataType() == DataType.STRING) {
            int length = entryArray.length;
            for (int i = 0; i < length; i++) {
              if (entryArray[i] != null) {
                entryArray[i] = entryArray[i].toString();
              }
            }
          }
          entry = entryArray;
        } else {
          if (incomingFieldSpec.getDataType() == DataType.STRING) {
            entry = entry.toString();
          }
        }
      } else {
        // Entry is null
        if (incomingFieldSpec.isSingleValueField()) {
          // Single-value field
          entry = AvroRecordReader.getDefaultNullValue(incomingFieldSpec);
        } else {
          // Multi-value field
          entry = new Object[]{AvroRecordReader.getDefaultNullValue(incomingFieldSpec)};
        }
      }
      destination.putField(columnName, entry);
    }

    return destination;
  }
}
