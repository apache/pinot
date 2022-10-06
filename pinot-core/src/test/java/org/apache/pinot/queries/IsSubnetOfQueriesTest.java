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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Queries test for Subnet Containment for IP Address queries.
 */
public class IsSubnetOfQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "IsSubnetOfQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String IPv4_PREFIX_COLUMN_STRING = "IPv4prefixColumn";
  private static final String IPv6_PREFIX_COLUMN_STRING = "IPv6prefixColumn";
  private static final String IPv4_ADDRESS_COLUMN = "IPv4AddressColumn";
  private static final String IPv6_ADDRESS_COLUMN = "IPv6AddressColumn";
  // columns storing hardcoded expected results:
  // isSubnetOf(IPv4prefixColumn, IPv4AddressColumn), isSubnetOf(IPv6prefixColumn, IPv6AddressColumn)
  private static final String IPv4_CONTAINS_COLUMN = "IPv4ContainsColumn";
  private static final String IPv6_CONTAINS_COLUMN = "IPv6ContainsColumn";

  private static final String DEFAULT_IPv4_PREFIX = "1.2.3.128/26";
  private static final String DEFAULT_IPv6_PREFIX = "64:fa9b::17/64";
  private static final String DEFAULT_IPv4_ADDRESS = "1.2.3.129";
  private static final String DEFAULT_IPv6_ADDRESS = "64:ffff::17";
  private static final boolean DEFAULT_IPv4_CONTAINS = true;
  private static final boolean DEFAULT_IPv6_CONTAINS = false;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().
      addSingleValueDimension(IPv4_PREFIX_COLUMN_STRING, FieldSpec.DataType.STRING)
      .addSingleValueDimension(IPv6_PREFIX_COLUMN_STRING, FieldSpec.DataType.STRING)
      .addSingleValueDimension(IPv4_ADDRESS_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(IPv6_ADDRESS_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(IPv4_CONTAINS_COLUMN, FieldSpec.DataType.BOOLEAN)
      .addSingleValueDimension(IPv6_CONTAINS_COLUMN, FieldSpec.DataType.BOOLEAN).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>();

    // add IPv4 test cases
    addIPv4Row(records, "10.34.0.32/27", "10.34.0.40", true);
    addIPv4Row(records, "10.32.229.160/28", "10.32.229.166", true);
    addIPv4Row(records, "10.154.116.0/26", "10.154.116.21", true);
    addIPv4Row(records, "144.2.15.0/26", "144.2.15.28", true);
    addIPv4Row(records, "10.252.186.64/26", "10.252.186.79", true);
    addIPv4Row(records, "10.187.84.128/26", "10.187.84.178", true);

    // add IPv6 test cases
    addIPv6Row(records, "2a04:f547:255:1::/64", "2a04:f547:255:1::2050", true);
    addIPv6Row(records, "2620:119:50e7:101::/64", "2620:119:50e7:101::9002:e15", true);
    addIPv6Row(records, "2001:db8:85a3::8a2e:370:7334/62", "2001:0db8:85a3:0003:ffff:ffff:ffff:ffff", true);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testIsSubnetOf() {

    // called in select
    String query = String.format(
        "select isSubnetOf(%s, %s) as IPv4Result, isSubnetOf(%s, %s) as IPv6Result, %s, %s from %s limit 100",
        IPv4_PREFIX_COLUMN_STRING, IPv4_ADDRESS_COLUMN, IPv6_PREFIX_COLUMN_STRING, IPv6_ADDRESS_COLUMN,
        IPv4_CONTAINS_COLUMN, IPv6_CONTAINS_COLUMN, RAW_TABLE_NAME);
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.BOOLEAN,
            DataSchema.ColumnDataType.BOOLEAN, DataSchema.ColumnDataType.BOOLEAN});
    List<Object[]> rows = resultTable.getRows();
    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);
      boolean IPv4Result = (boolean) row[0];
      boolean IPv6Result = (boolean) row[1];
      boolean expectedIPv4Result = (boolean) row[2];
      boolean expectedIPv6Result = (boolean) row[3];
      assertEquals(IPv4Result, expectedIPv4Result);
      assertEquals(IPv6Result, expectedIPv6Result);
    }
  }


  private void addIPv4Row(List<GenericRow> records, String prefix, String address, boolean expectedBool) {
    // add lib call assertEquals(expectedBool, );
    GenericRow record = new GenericRow();
    record.putValue(IPv4_PREFIX_COLUMN_STRING, prefix);
    record.putValue(IPv4_ADDRESS_COLUMN, address);
    record.putValue(IPv4_CONTAINS_COLUMN, expectedBool);

    record.putValue(IPv6_PREFIX_COLUMN_STRING, DEFAULT_IPv6_PREFIX);
    record.putValue(IPv6_ADDRESS_COLUMN, DEFAULT_IPv6_ADDRESS);
    record.putValue(IPv6_CONTAINS_COLUMN, DEFAULT_IPv6_CONTAINS);
    records.add(record);
  }

  private void addIPv6Row(List<GenericRow> records, String prefix, String address, boolean expectedBool) {
    // add lib call assertEquals(expectedBool, );
    GenericRow record = new GenericRow();
    record.putValue(IPv6_PREFIX_COLUMN_STRING, prefix);
    record.putValue(IPv6_ADDRESS_COLUMN, address);
    record.putValue(IPv6_CONTAINS_COLUMN, expectedBool);

    record.putValue(IPv4_PREFIX_COLUMN_STRING, DEFAULT_IPv4_PREFIX);
    record.putValue(IPv4_ADDRESS_COLUMN, DEFAULT_IPv4_ADDRESS);
    record.putValue(IPv4_CONTAINS_COLUMN, DEFAULT_IPv4_CONTAINS);
    records.add(record);
  }


  @Override
  protected String getFilter() {
    return null;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
