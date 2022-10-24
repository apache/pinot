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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.createFixed;
import static org.apache.avro.Schema.createRecord;


public class AvroRecordExtractorV2Test extends AvroRecordExtractorTest {

  @Override
  protected AvroRecordExtractor createAvroRecordExtractor() {
    return new AvroRecordExtractorV2();
  }

  @Test
  public void testFixedDataType() {
    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    Schema fixedSchema = createFixed("fixedSchema", "", "", 4);
    avroSchema.setFields(Lists.newArrayList(new Schema.Field("fixed_data", fixedSchema)));
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("fixed_data", new GenericData.Fixed(fixedSchema, new byte[] {0, 1, 2, 3}));
    GenericRow genericRow = new GenericRow();
    AvroRecordExtractor avroRecordExtractor = createAvroRecordExtractor();
    avroRecordExtractor.init(null, null);
    avroRecordExtractor.extract(genericRecord, genericRow);
    Assert.assertEquals(genericRow.getValue("fixed_data"), new byte[] {0, 1, 2, 3});
  }
}
