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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroRecordToPinotRowGeneratorTest {

  @Test
  public void testIncomingTimeColumn()
      throws Exception {
    List<Schema.Field> avroFields =
        Collections.singletonList(new Schema.Field("incomingTime", Schema.create(Schema.Type.LONG), null, null));
    Schema avroSchema = Schema.createRecord(avroFields);
    GenericData.Record avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("incomingTime", 12345L);

    Set<String> sourceFields = Sets.newHashSet("incomingTime", "outgoingTime");

    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(sourceFields, null);
    GenericRow genericRow = new GenericRow();
    avroRecordExtractor.extract(avroRecord, genericRow);

    Assert.assertTrue(
        genericRow.getFieldToValueMap().keySet().containsAll(Arrays.asList("incomingTime", "outgoingTime")));
    Assert.assertEquals(genericRow.getValue("incomingTime"), 12345L);
  }

  @Test
  public void testNoDifferentiateNullAndEmptyForMultiValueFields() {
    AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, config);

    Schema schema =
        SchemaBuilder.record("GenericRow").fields().name("arrField1").type().array().items().stringType().noDefault()
            .name("arrField2").type().array().items().stringType().noDefault().name("arrField3").type().array().items()
            .stringType().noDefault().endRecord();

    GenericRecord genericRecord = new GenericData.Record(schema);
    List<String> arrayData1 = Arrays.asList("value1", "value2", "value3");
    List<String> arrayData2 = null;
    List<String> arrayData3 = new ArrayList<>();

    GenericRow genericRow = new GenericRow();
    genericRecord.put("arrField1", arrayData1);
    genericRecord.put("arrField2", arrayData2);
    genericRecord.put("arrField3", arrayData3);

    avroRecordExtractor.extract(genericRecord, genericRow);
    Assert.assertTrue(
        genericRow.getFieldToValueMap().keySet().containsAll(Arrays.asList("arrField1", "arrField2", "arrField3")));
    Assert.assertEquals(genericRow.getValue("arrField1"), arrayData1.toArray());
    Assert.assertEquals(genericRow.getValue("arrField2"), null);
    Assert.assertEquals(genericRow.getValue("arrField3"), null);
  }

  @Test
  public void testDifferentiateNullAndEmptyForMultiValueFields() {
    AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
    config.setDifferentiateNullAndEmptyForMV(true);
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, config);

    Schema schema =
        SchemaBuilder.record("GenericRow").fields().name("arrField1").type().array().items().stringType().noDefault()
            .name("arrField2").type().array().items().stringType().noDefault().name("arrField3").type().array().items()
            .stringType().noDefault().endRecord();

    GenericRecord genericRecord = new GenericData.Record(schema);
    List<String> arrayData1 = Arrays.asList("value1", "value2", "value3");
    List<String> arrayData2 = null;
    List<String> arrayData3 = new ArrayList<>();

    GenericRow genericRow = new GenericRow();
    genericRecord.put("arrField1", arrayData1);
    genericRecord.put("arrField2", arrayData2);
    genericRecord.put("arrField3", arrayData3);

    avroRecordExtractor.extract(genericRecord, genericRow);
    Assert.assertTrue(
        genericRow.getFieldToValueMap().keySet().containsAll(Arrays.asList("arrField1", "arrField2", "arrField3")));
    Assert.assertEquals(genericRow.getValue("arrField1"), arrayData1.toArray());
    Assert.assertEquals(genericRow.getValue("arrField2"), null);
    Assert.assertEquals(genericRow.getValue("arrField3"), new String[0]);
  }
}
