package org.apache.pinot.queries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class JsonMalformedTest extends BaseQueriesTest {


    static final String RAW_TABLE_NAME = "testTable";
    static final String SEGMENT_NAME = "testSegment";

    static final String STRING_COLUMN = "stringColumn";
    static final String JSON_COLUMN = "jsonColumn";

    private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
            .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
            .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.STRING).build();

    private static final TableConfig TABLE_CONFIG =
            new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
                    .build();

    protected IndexSegment _indexSegment;
    protected List<IndexSegment> _indexSegments;
    List<GenericRow> records = new ArrayList<>();

    @BeforeClass
    public void setUp()
            throws Exception {
        records.add(createRecord("ludwik von drake",
                "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, \"data\": [\"l\", \"b\", \"c\", "
                        + "\"d\"]"));
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

    @Test
    public void testJsonIndexBuild() throws Exception {
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
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
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
