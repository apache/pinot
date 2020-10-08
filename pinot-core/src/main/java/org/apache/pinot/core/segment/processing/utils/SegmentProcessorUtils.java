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
package org.apache.pinot.core.segment.processing.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Helper util methods for SegmentProcessorFramework
 */
public final class SegmentProcessorUtils {

  private SegmentProcessorUtils() {
  }

  /**
   * Convert a GenericRow to an avro GenericRecord
   */
  public static GenericData.Record convertGenericRowToAvroRecord(GenericRow genericRow,
      GenericData.Record reusableRecord) {
    for (String field : genericRow.getFieldToValueMap().keySet()) {
      Object value = genericRow.getValue(field);
      if (value instanceof Object[]) {
        reusableRecord.put(field, Arrays.asList((Object[]) value));
      } else {
        if (value instanceof byte[]) {
          value = ByteBuffer.wrap((byte[]) value);
        }
        reusableRecord.put(field, value);
      }
    }
    return reusableRecord;
  }

  /**
   * Converts a Pinot schema to an Avro schema
   */
  public static Schema convertPinotSchemaToAvroSchema(org.apache.pinot.spi.data.Schema pinotSchema) {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler = SchemaBuilder.record("record").fields();

    for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
      String name = fieldSpec.getName();
      FieldSpec.DataType dataType = fieldSpec.getDataType();
      if (fieldSpec.isSingleValueField()) {
        switch (dataType) {
          case INT:
            fieldAssembler = fieldAssembler.name(name).type().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(name).type().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(name).type().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(name).type().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(name).type().stringType().noDefault();
            break;
          case BYTES:
            fieldAssembler = fieldAssembler.name(name).type().bytesType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + dataType);
        }
      } else {
        switch (dataType) {
          case INT:
            fieldAssembler = fieldAssembler.name(name).type().array().items().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(name).type().array().items().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(name).type().array().items().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(name).type().array().items().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(name).type().array().items().stringType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + dataType);
        }
      }
    }
    return fieldAssembler.endRecord();
  }
}
