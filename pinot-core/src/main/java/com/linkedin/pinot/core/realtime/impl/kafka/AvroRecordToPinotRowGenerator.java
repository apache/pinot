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

import com.linkedin.pinot.common.data.TimeFieldSpec;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;


public class AvroRecordToPinotRowGenerator {
  private final Schema indexingSchema;
  private final String incomingTimeColumnName;
  private final FieldSpec incomingTimeFieldSpec;

  public AvroRecordToPinotRowGenerator(Schema indexingSchema) {
    this.indexingSchema = indexingSchema;
    incomingTimeColumnName = indexingSchema.getTimeFieldSpec().getIncomingTimeColumnName();
    if (indexingSchema.getTimeColumnName().equals(incomingTimeColumnName)) {
      incomingTimeFieldSpec = new TimeFieldSpec(indexingSchema.getTimeFieldSpec().getIncomingGranularitySpec());
    } else {
      incomingTimeFieldSpec = indexingSchema.getTimeFieldSpec();
    }
  }

  public GenericRow transform(GenericData.Record record, org.apache.avro.Schema schema, GenericRow destination) {
    FieldSpec fieldSpec;
    for (String column : indexingSchema.getColumnNames()) {
      if (column.equals(indexingSchema.getTimeFieldSpec().getOutgoingTimeColumnName())) {
        column = incomingTimeColumnName;
        fieldSpec = incomingTimeFieldSpec;
      } else {
        fieldSpec = indexingSchema.getFieldSpecFor(column);
      }
      Object entry = record.get(column);

      if (entry != null) {
        if (entry instanceof Array) {
          entry = AvroRecordReader.transformAvroArrayToObjectArray((Array) entry, fieldSpec);
          if (fieldSpec.getDataType() == DataType.STRING || fieldSpec.getDataType() == DataType.STRING_ARRAY) {
            for (int i = 0; i < ((Object[]) entry).length; ++i) {
              if (((Object[]) entry)[i] != null) {
                ((Object[]) entry)[i] = ((Object[]) entry)[i].toString();
              }
            }
          }
        } else {
          if (entry instanceof Utf8) {
            entry = ((Utf8) entry).toString();
          }
          if (fieldSpec.getDataType() == DataType.STRING) {
            entry = entry.toString();
          }
        }
      } else {
        // entry was null.
        if (fieldSpec.isSingleValueField()) {
          entry = AvroRecordReader.getDefaultNullValue(fieldSpec);
        } else {
          // A multi-value field, and null. Any of the instanceof checks above will not match, so we need to repeat some
          // of the logic above here.
          entry = AvroRecordReader.transformAvroArrayToObjectArray((Array) entry, fieldSpec);
          if (fieldSpec.getDataType() == DataType.STRING || fieldSpec.getDataType() == DataType.STRING_ARRAY) {
            for (int i = 0; i < ((Object[]) entry).length; ++i) {
              if (((Object[]) entry)[i] != null) {
                ((Object[]) entry)[i] = ((Object[]) entry)[i].toString();
              }
            }
          }
        }
      }
      destination.putField(column, entry);
    }

    return destination;
  }
}
