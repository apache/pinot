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

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentProcessingUtilsTest {
  private Schema _avroSchema;
  private org.apache.pinot.spi.data.Schema _pinotSchema;

  @Test
  public void  pinotSchemaToAvroSchemaConversionTest(){
    _pinotSchema = new org.apache.pinot.spi.data.Schema.SchemaBuilder().setSchemaName("mySchema")
        .addSingleValueDimension("nullableIntField", DataType.INT).build();
    _avroSchema = SegmentProcessorUtils.convertPinotSchemaToAvroSchema(_pinotSchema);
    final Field field = _avroSchema.getField("nullableIntField");
    Assert.assertEquals(field.schema().getType(), Type.INT);
    GenericRecord record  = new GenericRecordBuilder(_avroSchema).set("nullableIntField", Integer.valueOf(0)).build();
  }

}
