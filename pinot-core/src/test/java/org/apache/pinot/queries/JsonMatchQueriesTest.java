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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for JSON_MATCH predicate.
 */
public class JsonMatchQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonMatchQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String ID_COLUMN = "id";
  private static final String JSON_COLUMN = "json";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(ID_COLUMN, DataType.INT)
      .addSingleValueDimension(JSON_COLUMN, DataType.JSON).build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setJsonIndexColumns(Collections.singletonList(JSON_COLUMN)).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>();
    // Top-level value
    records.add(createRecord(0, null));
    records.add(createRecord(1, 1));
    records.add(createRecord(2, "\"foo\""));
    records.add(createRecord(3, true));
    // Top-level array
    records.add(createRecord(4, "[1, 2, 3]"));
    records.add(createRecord(5, "[1, \"foo\", true]"));
    // Top-level nested-array
    records.add(createRecord(6, "[1, [\"foo\", true]]"));
    // Top-level array with object elements
    records.add(createRecord(7, "[{\"key\": 1}, {\"key\": \"foo\"}]"));
    // Top-level object
    records.add(createRecord(8, "{\"key\": null}"));
    records.add(createRecord(9, "{\"key\": 1}"));
    records.add(createRecord(10, "{\"key\": \"foo\"}"));
    records.add(createRecord(11, "{\"key\": true}"));
    // Top-level object with nested-array value
    records.add(createRecord(12, "{\"key\": [1, [\"foo\", true]]}"));
    // Top-level object with multiple nested-array values
    records.add(createRecord(13, "{\"key\": [1, [\"foo\", [true]]], \"key2\": [2, [\"bar\", false]]}"));

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setJsonIndexColumns(Collections.singleton(JSON_COLUMN));
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private GenericRow createRecord(int id, Object value) {
    GenericRow record = new GenericRow();
    record.putValue(ID_COLUMN, id);
    record.putValue(JSON_COLUMN, value);
    return record;
  }

  @Test
  public void testQueries() {
    // Top-level value
    assertEquals(getSelectedIds("'\"$\"=1'"), Collections.singleton(1));
    assertEquals(getSelectedIds("'\"$\"=''foo'''"), Collections.singleton(2));
    assertEquals(getSelectedIds("'\"$\"=true'"), Collections.singleton(3));
    assertEquals(getSelectedIds("'\"$\" IN (1, ''foo'')'"), new HashSet<>(Arrays.asList(1, 2)));
    assertEquals(getSelectedIds("'\"$\" IS NOT NULL'"), new HashSet<>(Arrays.asList(1, 2, 3)));

    // Top-level array
    assertEquals(getSelectedIds("'\"$[0]\"=1'"), new HashSet<>(Arrays.asList(4, 5, 6)));
    assertEquals(getSelectedIds("'\"$[*]\"=2'"), Collections.singleton(4));
    assertEquals(getSelectedIds("'\"$[*]\"=''foo'''"), Collections.singleton(5));
    assertEquals(getSelectedIds("'\"$[2]\"=true'"), Collections.singleton(5));

    // Top-level nested-array
    assertEquals(getSelectedIds("'\"$[*][*]\"=true'"), Collections.singleton(6));
    assertEquals(getSelectedIds("'\"$[*][0]\"=''foo'''"), Collections.singleton(6));
    assertEquals(getSelectedIds("'\"$[1][*]\"=true'"), Collections.singleton(6));
    assertEquals(getSelectedIds("'\"$[1][0]\"=''foo'''"), Collections.singleton(6));
    assertTrue(getSelectedIds("'\"$[*][*]\"=1'").isEmpty());

    // Top-level array with object elements
    assertEquals(getSelectedIds("'\"$[*].key\"=1'"), Collections.singleton(7));
    assertEquals(getSelectedIds("'\"$[1].key\"=''foo'''"), Collections.singleton(7));
    assertTrue(getSelectedIds("'\"$[*].foo\"=1'").isEmpty());

    // Top-level object
    assertEquals(getSelectedIds("'\"$.key\"=1'"), Collections.singleton(9));
    assertEquals(getSelectedIds("'\"$.key\"=''foo'''"), Collections.singleton(10));
    assertEquals(getSelectedIds("'\"$.key\"=true'"), Collections.singleton(11));
    assertEquals(getSelectedIds("'\"$.key\" IN (1, ''foo'')'"), new HashSet<>(Arrays.asList(9, 10)));
    assertEquals(getSelectedIds("'\"$.key\" IS NOT NULL'"), new HashSet<>(Arrays.asList(9, 10, 11)));

    // Top-level object with nested-array value
    assertEquals(getSelectedIds("'\"$.key[0]\"=1'"), new HashSet<>(Arrays.asList(12, 13)));
    assertEquals(getSelectedIds("'\"$.key[*][0]\"=''foo'''"), new HashSet<>(Arrays.asList(12, 13)));
    assertEquals(getSelectedIds("'\"$.key[1][*]\"=true'"), Collections.singleton(12));
    assertEquals(getSelectedIds("'\"$.key[1][1][0]\"=true'"), Collections.singleton(13));

    // Top-level object with multiple nested-array values
    assertEquals(getSelectedIds("'\"$.key[*][*][*]\"=true AND \"$.key2[1][0]\"=''bar'''"), Collections.singleton(13));

    // Legacy query format
    assertEquals(getSelectedIds("'key=1'"), Collections.singleton(9));
    assertEquals(getSelectedIds("'key=''foo'''"), Collections.singleton(10));
    assertEquals(getSelectedIds("'key=true'"), Collections.singleton(11));
    assertEquals(getSelectedIds("'key IN (1, ''foo'')'"), new HashSet<>(Arrays.asList(9, 10)));
    assertEquals(getSelectedIds("'key IS NOT NULL'"), new HashSet<>(Arrays.asList(9, 10, 11)));
    assertEquals(getSelectedIds("'\"key[0]\"=1'"), new HashSet<>(Arrays.asList(12, 13)));
    assertEquals(getSelectedIds("'\"key[*][0]\"=''foo'''"), new HashSet<>(Arrays.asList(12, 13)));
    assertEquals(getSelectedIds("'\"key[1][*]\"=true'"), Collections.singleton(12));
    assertEquals(getSelectedIds("'\"key[1][1][0]\"=true'"), Collections.singleton(13));
  }

  private Set<Integer> getSelectedIds(String jsonMatchExpression) {
    String query = String.format("SELECT id FROM testTable WHERE JSON_MATCH(json, %s) LIMIT 100", jsonMatchExpression);
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    Set<Integer> selectedIds = new HashSet<>();
    for (Object[] row : rows) {
      selectedIds.add((Integer) row[0]);
    }
    return selectedIds;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
