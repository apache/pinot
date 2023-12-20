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

import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.ColumnJsonParserException;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class JsonMalformedIndexTest extends BaseQueriesTest {
    private static final String RAW_TABLE_NAME = "testTable";
    private static final String SEGMENT_NAME = "testSegment";
    private static final String STRING_COLUMN = "stringColumn";
    private static final String JSON_COLUMN = "jsonColumn";
    private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
            .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
            .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.STRING).build();
    private static final TableConfig TABLE_CONFIG =
            new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    private IndexSegment _indexSegment;
    private List<IndexSegment> _indexSegments;
    private final List<GenericRow> _records = new ArrayList<>();

    @BeforeClass
    public void setUp()
            throws Exception {
        _records.add(createRecord("ludwik von drake",
                "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, "
                        + "\"data\": [\"l\", \"b\", \"c\", \"d\"]"));
    }

    protected void checkResult(String query, Object[][] expectedResults) {
        BrokerResponseNative brokerResponse = getBrokerResponseForOptimizedQuery(query, TABLE_CONFIG, SCHEMA);
        QueriesTestUtils.testInterSegmentsResult(brokerResponse, Arrays.asList(expectedResults));
    }

    File indexDir() {
        return new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
    }

    GenericRow createRecord(String stringValue, String jsonValue) {
        GenericRow record = new GenericRow();
        record.putValue(STRING_COLUMN, stringValue);
        record.putValue(JSON_COLUMN, jsonValue);
        return record;
    }

    @Test(expectedExceptions = ColumnJsonParserException.class,
          expectedExceptionsMessageRegExp = "Column: jsonColumn.*")
    public void testJsonIndexBuild()
            throws Exception {
        File indexDir = indexDir();
        FileUtils.deleteDirectory(indexDir);

        List<String> jsonIndexColumns = new ArrayList<>();
        jsonIndexColumns.add("jsonColumn");
        TABLE_CONFIG.getIndexingConfig().setJsonIndexColumns(jsonIndexColumns);
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
        segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
        segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
        segmentGeneratorConfig.setOutDir(indexDir.getPath());

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(_records));
        driver.build();

        IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
        indexLoadingConfig.setTableConfig(TABLE_CONFIG);
        indexLoadingConfig.setJsonIndexColumns(new HashSet<>(jsonIndexColumns));
        indexLoadingConfig.setReadMode(ReadMode.mmap);

        ImmutableSegment immutableSegment =
                ImmutableSegmentLoader.load(new File(indexDir, SEGMENT_NAME), indexLoadingConfig);
        _indexSegment = immutableSegment;
        _indexSegments = Arrays.asList(immutableSegment, immutableSegment);

        Object[][] expecteds1 = {{"von drake"}, {"von drake"}, {"von drake"}, {"von drake"}};
        checkResult("SELECT jsonextractscalar(jsonColumn, '$.name.last', 'STRING') FROM testTable", expecteds1);
    }

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
}
