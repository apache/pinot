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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;
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

    org.apache.pinot.spi.data.Schema pinotSchema =
        new org.apache.pinot.spi.data.Schema.SchemaBuilder().setSchemaName("testSchema")
            .addTime("incomingTime", TimeUnit.MILLISECONDS, FieldSpec.DataType.LONG, "outgoingTime", TimeUnit.DAYS,
                FieldSpec.DataType.INT).build();
    List<String> sourceFieldNames = SchemaFieldExtractorUtils.extract(pinotSchema);

    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(sourceFieldNames, null);
    GenericRow genericRow = new GenericRow();
    avroRecordExtractor.extract(avroRecord, genericRow);

    Assert.assertTrue(
        genericRow.getFieldToValueMap().keySet().containsAll(Arrays.asList("incomingTime", "outgoingTime")));
    Assert.assertEquals(genericRow.getValue("incomingTime"), 12345L);
  }
}
