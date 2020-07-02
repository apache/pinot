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
package org.apache.pinot.plugin.inputformat.thrift;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test {@code org.apache.pinot.plugin.inputformat.thrift.data.ThriftRecordReader} for a given sample thrift
 * data.
 */
public class ThriftRecordReaderTest {
  private static final String THRIFT_DATA = "_test_sample_thrift_data.thrift";

  private File _tempFile;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(_tempFile);

    ThriftSampleData t1 = new ThriftSampleData();
    t1.setActive(true);
    t1.setCreated_at(1515541280L);
    t1.setId(1);
    t1.setName("name1");
    List<Short> t1Groups = new ArrayList<>(2);
    t1Groups.add((short) 1);
    t1Groups.add((short) 4);
    t1.setGroups(t1Groups);
    Map<String, Long> mapValues = new HashMap<>();
    mapValues.put("name1", 1L);
    t1.setMap_values(mapValues);
    Set<String> namesSet = new HashSet<>();
    namesSet.add("name1");
    t1.setSet_values(namesSet);

    ThriftSampleData t2 = new ThriftSampleData();
    t2.setActive(false);
    t2.setCreated_at(1515541290L);
    t2.setId(2);
    t2.setName("name2");
    List<Short> t2Groups = new ArrayList<>(2);
    t2Groups.add((short) 2);
    t2Groups.add((short) 3);
    t2.setGroups(t2Groups);
    List<ThriftSampleData> lists = new ArrayList<>(2);
    lists.add(t1);
    lists.add(t2);
    TSerializer binarySerializer = new TSerializer(new TBinaryProtocol.Factory());
    _tempFile = getSampleDataPath();
    FileWriter writer = new FileWriter(_tempFile);
    for (ThriftSampleData d : lists) {
      IOUtils.write(binarySerializer.serialize(d), writer);
    }
    writer.close();
  }

  @Test
  public void testReadData()
      throws IOException {
    ThriftRecordReader recordReader = new ThriftRecordReader();
    recordReader.init(_tempFile, getSourceFields(), getThriftRecordReaderConfig());
    List<GenericRow> genericRows = new ArrayList<>();
    while (recordReader.hasNext()) {
      genericRows.add(recordReader.next());
    }
    recordReader.close();
    Assert.assertEquals(genericRows.size(), 2, "The number of rows return is incorrect");
    int id = 1;
    for (GenericRow outputRow : genericRows) {
      Assert.assertEquals(outputRow.getValue("id"), id);
      Assert.assertNull(outputRow.getValue("map_values"));
      id++;
    }
  }

  @Test
  public void testRewind()
      throws IOException {
    ThriftRecordReader recordReader = new ThriftRecordReader();
    recordReader.init(_tempFile, getSourceFields(), getThriftRecordReaderConfig());
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

  private File getSampleDataPath()
      throws IOException {
    return File.createTempFile(ThriftRecordReaderTest.class.getName(), THRIFT_DATA);
  }

  private ThriftRecordReaderConfig getThriftRecordReaderConfig() {
    ThriftRecordReaderConfig config = new ThriftRecordReaderConfig();
    config.setThriftClass("org.apache.pinot.plugin.inputformat.thrift.ThriftSampleData");
    return config;
  }

  private Schema getSchema() {
    return new Schema.SchemaBuilder().setSchemaName("ThriftSampleData")
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("created_at", FieldSpec.DataType.LONG)
        .addSingleValueDimension("active", FieldSpec.DataType.BOOLEAN)
        .addMultiValueDimension("groups", FieldSpec.DataType.INT)
        .addMultiValueDimension("set_values", FieldSpec.DataType.STRING).build();
  }

  private Set<String> getSourceFields() {
    return Sets.newHashSet("id", "name", "created_at", "active", "groups", "set_values");
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_tempFile);
  }
}
