package org.apache.pinot.queries;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
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
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for DISTINCT with Dictionary based plan enabled
 */
public class DistinctQueriesWithDictionaryTest extends BaseQueriesWithDictBasedDistinctEnabledTest {
    private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctQueriesWithDictionaryTest");
    private static final String RAW_TABLE_NAME = "testTable";
    private static final String SEGMENT_NAME_PREFIX = "testSegment_";

    private static final int NUM_RECORDS_PER_SEGMENT = 10000;
    private static final int NUM_UNIQUE_RECORDS_PER_SEGMENT = 100;

    private static final String INT_COLUMN = "intColumn";
    private static final String LONG_COLUMN = "longColumn";
    private static final String FLOAT_COLUMN = "floatColumn";
    private static final String DOUBLE_COLUMN = "doubleColumn";
    private static final String STRING_COLUMN = "stringColumn";
    private static final String BYTES_COLUMN = "bytesColumn";
    private static final String RAW_INT_COLUMN = "rawIntColumn";
    private static final String RAW_LONG_COLUMN = "rawLongColumn";
    private static final String RAW_FLOAT_COLUMN = "rawFloatColumn";
    private static final String RAW_DOUBLE_COLUMN = "rawDoubleColumn";
    private static final String RAW_STRING_COLUMN = "rawStringColumn";
    private static final String RAW_BYTES_COLUMN = "rawBytesColumn";
    private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
            .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
            .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
            .addSingleValueDimension(BYTES_COLUMN, FieldSpec.DataType.BYTES).addSingleValueDimension(RAW_INT_COLUMN, FieldSpec.DataType.INT)
            .addSingleValueDimension(RAW_LONG_COLUMN, FieldSpec.DataType.LONG).addSingleValueDimension(RAW_FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
            .addSingleValueDimension(RAW_DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
            .addSingleValueDimension(RAW_STRING_COLUMN, FieldSpec.DataType.STRING)
            .addSingleValueDimension(RAW_BYTES_COLUMN, FieldSpec.DataType.BYTES).build();
    private static final TableConfig TABLE = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
            .setNoDictionaryColumns(Arrays
                    .asList(RAW_INT_COLUMN, RAW_LONG_COLUMN, RAW_FLOAT_COLUMN, RAW_DOUBLE_COLUMN, RAW_STRING_COLUMN,
                            RAW_BYTES_COLUMN)).build();

    private IndexSegment _indexSegment;
    private List<IndexSegment> _indexSegments;

    @Override
    protected String getFilter() {
        throw new UnsupportedOperationException();
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
        FileUtils.deleteQuietly(INDEX_DIR);

        ImmutableSegment segment0 = createSegment(0, generateRecords(0));
        ImmutableSegment segment1 = createSegment(1, generateRecords(1000));
        _indexSegment = segment0;
        _indexSegments = Arrays.asList(segment0, segment1);
    }

    @AfterClass
    public void tearDown() {
        for (IndexSegment indexSegment : _indexSegments) {
            indexSegment.destroy();
        }

        FileUtils.deleteQuietly(INDEX_DIR);
    }

    @Test
    public void testSingleColumnDistinctOnly() {
        {
            // Numeric columns
            //@formatter:off
            List<String> queries = Arrays
                    .asList("SELECT DISTINCT(intColumn) FROM testTable", "SELECT DISTINCT(longColumn) FROM testTable",
                            "SELECT DISTINCT(floatColumn) FROM testTable", "SELECT DISTINCT(doubleColumn) FROM testTable");
            //@formatter:on
            Set<Integer> expectedValues = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                expectedValues.add(i);
            }
            for (String query : queries) {
                AbstractCollection pqlDistinctSet = getAbstractCollection(query, true);
                AbstractCollection sqlDistinctSet = getAbstractCollection(query, false);
                for (AbstractCollection distinctSet : Arrays
                        .asList(pqlDistinctSet, sqlDistinctSet)) {
                    assertEquals(distinctSet.size(), 10);
                    Set<Integer> actualValues = new HashSet<>();
                    Iterator iterator = distinctSet.iterator();

                    while (iterator.hasNext()) {
                        Object value = iterator.next();

                        assertTrue(value instanceof Integer || value instanceof Long || value instanceof Double || value instanceof Float);
                        actualValues.add(((Number) value).intValue());
                    }
                    assertEquals(actualValues, expectedValues);
                }
            }
        }
        {
            // String columns
            //@formatter:off
            List<String> queries = Arrays
                    .asList("SELECT DISTINCT(stringColumn) FROM testTable LIMIT 100");
            //@formatter:on
            Set<Integer> expectedValues = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                expectedValues.add(i);
            }
            for (String query : queries) {
                AbstractCollection pqlDistinctSet = getAbstractCollection(query, true);
                AbstractCollection sqlDistinctSet = getAbstractCollection(query, false);

                for (AbstractCollection distinctSet : Arrays
                        .asList(pqlDistinctSet,  sqlDistinctSet)) {
                    assertEquals(distinctSet.size(), 100);
                    Set<Integer> actualValues = new HashSet<>();
                    Iterator iterator = distinctSet.iterator();

                    while (iterator.hasNext()) {
                        Object value = iterator.next();

                        assertTrue(value instanceof String);
                        actualValues.add(Integer.parseInt((String) value));
                    }
                    assertEquals(actualValues, expectedValues);
                }
            }
        }
        {
            // Bytes columns
            //@formatter:off
            List<String> queries = Arrays
                    .asList("SELECT DISTINCT(bytesColumn) FROM testTable");
            //@formatter:on
            Set<Integer> expectedValues = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                expectedValues.add(i);
            }
            for (String query : queries) {
                AbstractCollection pqlDistinctSet = getAbstractCollection(query, true);
                AbstractCollection sqlDistinctSet = getAbstractCollection(query, false);

                for (AbstractCollection distinctSet : Arrays
                        .asList(pqlDistinctSet, sqlDistinctSet)) {
                    assertEquals(distinctSet.size(), 10);
                    Set<Integer> actualValues = new HashSet<>();
                    Iterator iterator = distinctSet.iterator();
                    while (iterator.hasNext()){
                        Object value = iterator.next();

                        assertTrue(value instanceof ByteArray);
                        actualValues.add(Integer.parseInt(
                                org.apache.pinot.spi.utils.StringUtils.decodeUtf8(((ByteArray) value).getBytes()).trim()));
                    }
                    assertEquals(actualValues, expectedValues);
                }
            }
        }
    }

    @Test
    public void testDistinctInterSegmentWithDictionaryBasedPlan() {
        //@formatter:off
        String[] pqlQueries = new String[]{
                "SELECT DISTINCT(intColumn) FROM testTable LIMIT 10000",
                "SELECT DISTINCT(stringColumn) FROM testTable WHERE intColumn >= 60 LIMIT 10000",
                "SELECT DISTINCT(intColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5",
                "SELECT DISTINCT(ADD(intColumn, floatColumn), stringColumn) FROM testTable WHERE longColumn < 60 ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        };
        String[] sqlQueries = new String[]{"SELECT DISTINCT floatColumn FROM testTable LIMIT 10000",
                "SELECT DISTINCT stringColumn FROM testTable WHERE intColumn >= 60 LIMIT 10000",
                "SELECT DISTINCT intColumn FROM testTable ORDER BY rawBytesColumn LIMIT 5",
                "SELECT DISTINCT ADD(intColumn, floatColumn), stringColumn FROM testTable WHERE longColumn < 60 ORDER BY stringColumn DESC, ADD(intColumn, floatColumn) ASC LIMIT 10",
        };
        //@formatter:on
        testDistinctInterSegmentHelper(pqlQueries, sqlQueries);
    }

    /**
     * Helper method which tests single column DISTINCT queries with no predicates across two segments
     */
    private void testDistinctInterSegmentHelper(String[] pqlQueries, String[] sqlQueries) {
        {
            // Test selecting all columns
            String pqlQuery = pqlQueries[0];
            String sqlQuery = sqlQueries[0];

            // Check data schema
            BrokerResponseNative pqlResponse = getBrokerResponseForPqlQuery(pqlQuery, PLAN_MAKER_WITH_DICTAGG);
            SelectionResults selectionResults = pqlResponse.getSelectionResults();
            assertNotNull(selectionResults);
            assertEquals(selectionResults.getColumns(),
                    Arrays.asList("distinct_intColumn"));
            BrokerResponseNative sqlResponse = getBrokerResponseForSqlQuery(sqlQuery, PLAN_MAKER_WITH_DICTAGG);
            ResultTable resultTable = sqlResponse.getResultTable();
            assertNotNull(resultTable);
            DataSchema dataSchema = resultTable.getDataSchema();
            assertEquals(dataSchema.getColumnNames(),
                    new String[]{"distinct_floatColumn"});

            // Check values, where all 200 unique values should be returned
            List<Serializable[]> pqlRows = selectionResults.getRows();
            assertEquals(pqlRows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT);
            List<Object[]> sqlRows = resultTable.getRows();
            assertEquals(sqlRows.size(), 2 * NUM_UNIQUE_RECORDS_PER_SEGMENT);
            Set<Integer> expectedValues = new HashSet<>();
            for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
                expectedValues.add(i);
                expectedValues.add(1000 + i);
            }
            Set<Integer> pqlValues = new HashSet<>();
            for (Serializable[] row : pqlRows) {
                int intValue = (int) row[0];
                assertEquals(((Integer) row[0]).intValue(), intValue);
                pqlValues.add(intValue);
            }
            assertEquals(pqlValues, expectedValues);

            Set<Float> expectedValuesFloat = new HashSet<>();
            for (float i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
                expectedValuesFloat.add(i);
                expectedValuesFloat.add((float) (1000.0 + i));
            }

            Set<Float> sqlValues = new HashSet<>();
            for (Object[] row : sqlRows) {
                float floatValue = (float) row[0];
                assertEquals(((Float) row[0]).floatValue(), floatValue);
                sqlValues.add(floatValue);
            }
            assertEquals(sqlValues, expectedValuesFloat);
        }
    }

    /**
     * Helper method to generate records based on the given base value.
     *
     * All columns will have the same value but different data types (BYTES values are encoded STRING values).
     * For the {i}th unique record, the value will be {baseValue + i}.
     */
    private List<GenericRow> generateRecords(int baseValue) {
        List<GenericRow> uniqueRecords = new ArrayList<>(NUM_UNIQUE_RECORDS_PER_SEGMENT);
        for (int i = 0; i < NUM_UNIQUE_RECORDS_PER_SEGMENT; i++) {
            int value = baseValue + i;
            GenericRow record = new GenericRow();
            record.putValue(INT_COLUMN, value);
            record.putValue(LONG_COLUMN, (long) value);
            record.putValue(FLOAT_COLUMN, (float) value);
            record.putValue(DOUBLE_COLUMN, (double) value);
            String stringValue = Integer.toString(value);
            record.putValue(STRING_COLUMN, stringValue);
            record.putValue(BYTES_COLUMN, StringUtil.encodeUtf8(StringUtils.leftPad(stringValue, 4)));
            record.putValue(RAW_INT_COLUMN, value);
            record.putValue(RAW_LONG_COLUMN, (long) value);
            record.putValue(RAW_FLOAT_COLUMN, (float) value);
            record.putValue(RAW_DOUBLE_COLUMN, (double) value);
            record.putValue(RAW_STRING_COLUMN, stringValue);
            record.putValue(RAW_BYTES_COLUMN, StringUtil.encodeUtf8(stringValue));
            uniqueRecords.add(record);
        }

        List<GenericRow> records = new ArrayList<>(NUM_RECORDS_PER_SEGMENT);
        for (int i = 0; i < NUM_RECORDS_PER_SEGMENT; i += NUM_UNIQUE_RECORDS_PER_SEGMENT) {
            records.addAll(uniqueRecords);
        }
        return records;
    }

    private ImmutableSegment createSegment(int index, List<GenericRow> records)
            throws Exception {
        String segmentName = SEGMENT_NAME_PREFIX + index;

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE, SCHEMA);
        segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
        segmentGeneratorConfig.setSegmentName(segmentName);
        segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
        driver.build();

        return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
    }

    /**
     * Helper method to get the AbstractCollection result for one single segment for the given query.
     */
    private AbstractCollection getAbstractCollection(String query, boolean isPql) {
        DictionaryBasedAggregationOperator dictionaryBasedAggregationOperator = isPql ? getOperatorForPqlQueryWithDictBasedDistinct(query) : getOperatorForSqlQueryWithDictBasedDistinct(query);
        List<Object> operatorResult = dictionaryBasedAggregationOperator.nextBlock().getAggregationResult();
        assertNotNull(operatorResult);
        assertEquals(operatorResult.size(), 1);
        assertTrue(operatorResult.get(0) instanceof AbstractCollection);
        return (AbstractCollection) operatorResult.get(0);
    }
}
