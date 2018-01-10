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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.util.TestUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test {@code com.linkedin.pinot.core.data.readers.ThriftRecordReader} for a given sample thrift
 * data Sample code to generate thrift test data.
 *
 * <pre>
 * {@code
 * ThriftSampleData t1 = new ThriftSampleData();
 * t1.setActive(true);
 * t1.setCreated_at(1515541280L);
 * t1.setId(1);
 * t1.setName("name1");
 * List<Short> t1Groups = Lists.newArrayList((short) 2);
 * t1Groups.add((short)1);
 * t1Groups.add((short)4);
 * t1.setGroups(t1Groups);
 * ThriftSampleData t2 = new ThriftSampleData();
 * t2.setActive(false);
 * t2.setCreated_at(1515541290L);
 * t2.setId(2);
 * t2.setName("name2");
 * List<Short> t2Groups = Lists.newArrayList((short) 2);
 * t2Groups.add((short)2);
 * t2Groups.add((short)3);
 * t2.setGroups(t2Groups);
 * List<ThriftSampleData> lists = Lists.newArrayList(t1, t2);
 * TSerializer binarySerializer = new TSerializer(new TBinaryProtocol.Factory());
 * FileWriter writer = new FileWriter(getSampleDataPath());
 * for(ThriftSampleData d: lists) {
 * IOUtils.write(binarySerializer.serialize(d), writer);
 * }
 * writer.close();
 * }
 * </pre>
 */

public class ThriftRecordReaderTest {

    private final String THRIFT_DATA = "data/test_sample_thrift_data";

    @Test
    public void testReadData() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        ThriftRecordReader recordReader = new ThriftRecordReader(new File(getSampleDataPath()),
                getSchema(), getThriftRecordReaderConfig());

        List<GenericRow> genericRows = new ArrayList<>();
        while (recordReader.hasNext()) {
            genericRows.add(recordReader.next());
        }
        recordReader.close();
        Assert.assertEquals(genericRows.size(), 2, "The number of rows return is incorrect");
        int id = 1;
        for (GenericRow outputRow : genericRows) {
            Assert.assertEquals(outputRow.getValue("id"), id);
            id++;
        }
    }

    @Test
    public void testRewind() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        ThriftRecordReader recordReader = new ThriftRecordReader(new File(getSampleDataPath()),
                getSchema(), getThriftRecordReaderConfig());
        List<GenericRow> genericRows = new ArrayList<>();
        while (recordReader.hasNext()) {
            genericRows.add(recordReader.next());
        }

        recordReader.rewind();

        while (recordReader.hasNext()) {
            genericRows.add(recordReader.next());
        }
        recordReader.close();
        Assert.assertEquals(genericRows.size(), 4, "The number of rows return after the rewind is incorrect");
    }

    private String getSampleDataPath() {
        return TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(THRIFT_DATA));
    }

    private ThriftRecordReaderConfig getThriftRecordReaderConfig() {
        ThriftRecordReaderConfig config = new ThriftRecordReaderConfig();
        config.set_thriftClass("com.linkedin.pinot.core.data.readers.ThriftSampleData");
        return config;
    }

    private Schema getSchema() {
        return new Schema.SchemaBuilder()
                .setSchemaName("ThriftSampleData")
                .addSingleValueDimension("id", FieldSpec.DataType.INT)
                .addSingleValueDimension("name", FieldSpec.DataType.STRING)
                .addSingleValueDimension("created_at", FieldSpec.DataType.LONG)
                .addSingleValueDimension("active", FieldSpec.DataType.BOOLEAN)
                .addMultiValueDimension("groups", FieldSpec.DataType.SHORT)
                .build();
    }

}
