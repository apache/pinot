package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SumWithPrecisionTest extends BaseSingleValueQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SumWithPrecisionTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 1000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
          .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
          .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
          .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
          .addSingleValueDimension(BYTES_COLUMN, FieldSpec.DataType.BYTES).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private BigDecimal _aggregatedValuePerSegment;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    // NOTE: Use a match all filter to switch between DictionaryBasedAggregationOperator and AggregationOperator
    return " WHERE intColumn >= 0";
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
    _aggregatedValuePerSegment = new BigDecimal(0);
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, value);
      record.putValue(LONG_COLUMN, (long) value);
      record.putValue(FLOAT_COLUMN, (float) value);
      record.putValue(DOUBLE_COLUMN, (double) value);
      String stringValue = Integer.toString(value);
      record.putValue(STRING_COLUMN, stringValue);
      // NOTE: Create fixed-length bytes so that dictionary can be generated
      BigDecimal bigDecimalValueLeft = new BigDecimal(Double.toString(RANDOM.nextDouble()));
      BigDecimal bigDecimalValueRight = new BigDecimal(Double.toString(RANDOM.nextDouble()));
      BigDecimal bigDecimalValue = bigDecimalValueLeft.multiply(bigDecimalValueRight);

      _aggregatedValuePerSegment = _aggregatedValuePerSegment.add(bigDecimalValue);
      byte[] bytesValue = DataTypeConversionFunctions.bigDecimalToBytes(bigDecimalValue);
      record.putValue(BYTES_COLUMN, bytesValue);
      records.add(record);
    }

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
  public void testAggregationOnly() {
    String query = "SELECT SUMPRECISION(bytesColumn) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS,
        NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS,
        NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResult);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResult, aggregationResultWithFilter);
    assertEquals(aggregationResult.get(0), _aggregatedValuePerSegment);
  }

  @Test
  public void testAggregationWithPrecision() {
    String query = "SELECT SUMPRECISION(bytesColumn, 6) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS,
        NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, NUM_RECORDS,
        NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResult);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResult, aggregationResultWithFilter);
    assertTrue(_aggregatedValuePerSegment.subtract((BigDecimal) aggregationResult.get(0)).abs().doubleValue() <= 0.1);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
