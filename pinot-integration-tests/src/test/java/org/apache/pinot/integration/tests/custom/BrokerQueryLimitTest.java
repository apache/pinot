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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.*;

public class BrokerQueryLimitTest extends CustomDataQueryClusterIntegrationTest {

    private static final String LONG_COLUMN = "longCol";
    public static final int DEFAULT_LIMIT = 5;

    @Test
    public void testWhenLimitIsNOTSetExplicitlyThenDefaultLimitIsApplied() throws Exception {
        setUseMultiStageQueryEngine(false);
        String query = String.format("SELECT %s FROM %s", LONG_COLUMN, getTableName());

        JsonNode result = postQuery(query).get("resultTable");
        JsonNode columnDataTypesNode = result.get("dataSchema").get("columnDataTypes");
        assertEquals(columnDataTypesNode.get(0).textValue(), "LONG");

        JsonNode rows = result.get("rows");
        assertEquals(rows.size(), DEFAULT_LIMIT);

        for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
            JsonNode row = rows.get(rowNum);
            assertEquals(row.size(), 1);
            assertEquals(row.get(0).asLong(), rowNum );
        }
    }

    @Test
    public void testWhenLimitISSetExplicitlyThenDefaultLimitIsNotApplied() throws Exception {
        setUseMultiStageQueryEngine(false);
        String query = String.format("SELECT %s FROM %s limit 20", LONG_COLUMN, getTableName());

        JsonNode result = postQuery(query).get("resultTable");
        JsonNode columnDataTypesNode = result.get("dataSchema").get("columnDataTypes");
        assertEquals(columnDataTypesNode.get(0).textValue(), "LONG");

        JsonNode rows = result.get("rows");
        assertEquals(rows.size(), 20);

        for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
            JsonNode row = rows.get(rowNum);
            assertEquals(row.size(), 1);
            assertEquals(row.get(0).asLong(), rowNum );
        }
    }

    @Override
    protected void overrideBrokerConf(PinotConfiguration brokerConf) {
        brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_DEFAULT_QUERY_RESPONSE_LIMIT, DEFAULT_LIMIT);
    }

    @Override
    public String getTableName() {
        return DEFAULT_TABLE_NAME;
    }

    @Override
    public Schema createSchema() {
        return new Schema.SchemaBuilder().setSchemaName(getTableName())
                .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
                .build();
    }

    @Override
    public File createAvroFile() throws Exception {
        org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
        avroSchema.setFields(ImmutableList.of(
                new org.apache.avro.Schema.Field(LONG_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
                        null, null)));

        File avroFile = new File(_tempDir, "data.avro");
        try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
            writer.create(avroSchema, avroFile);
            for (int i = 0; i < getCountStarResult(); i++) {
                GenericData.Record record = new GenericData.Record(avroSchema);
                record.put(LONG_COLUMN, i);
                writer.append(record);
            }
        }
        return avroFile;
    }
}
