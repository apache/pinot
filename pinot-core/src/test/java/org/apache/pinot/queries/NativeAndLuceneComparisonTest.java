package org.apache.pinot.queries;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NativeAndLuceneComparisonTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_LUCENE = "testSegmentLucene";
  private static final String SEGMENT_NAME_NATIVE = "testSegmentNative";
  private static final String DOMAIN_NAMES_COL_LUCENE = "DOMAIN_NAMES_LUCENE";
  private static final String DOMAIN_NAMES_COL_NATIVE = "DOMAIN_NAMES_NATIVE";
  private static final Integer NUM_ROWS = 1024;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  private IndexSegment _luceneSegment;
  private IndexSegment _nativeIndexSegment;

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
    FileUtils.deleteQuietly(INDEX_DIR);

    List<IndexSegment> segments = new ArrayList<>();
    buildLuceneSegment();
    buildNativeTextIndexSegment();

    _luceneSegment = loadLuceneSegment();
    _nativeIndexSegment = loadNativeIndexSegment();

    segments.add(_luceneSegment);
    segments.add(_nativeIndexSegment);

    _indexSegment = segments.get(ThreadLocalRandom.current().nextInt(2));
    _indexSegments = segments;
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }


  private List<String> getDomainNames() {
    return Arrays
        .asList("www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd", "www.sd.domain1.com",
            "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd", "www.domain2.com",
            "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd", "www.sd.domain2.com", "www.sd.domain2.co.ab",
            "www.sd.domain2.co.bc", "www.sd.domain2.co.cd");
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      GenericRow row = new GenericRow();
      row.putField(DOMAIN_NAMES_COL_LUCENE, domain);
      row.putField(DOMAIN_NAMES_COL_NATIVE, domain);
      rows.add(row);
    }
    return rows;
  }

  private void buildLuceneSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_LUCENE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL_LUCENE, DOMAIN_NAMES_COL_NATIVE)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_LUCENE, FieldSpec.DataType.STRING)
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_LUCENE);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void buildNativeTextIndexSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    Map<String, String> propertiesMap = new HashMap<>();
    FSTType fstType = FSTType.NATIVE;

    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_NATIVE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL_LUCENE)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_NATIVE, FieldSpec.DataType.STRING)
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_NATIVE);
    config.setFSTIndexType(fstType);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private ImmutableSegment loadLuceneSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_LUCENE), indexLoadingConfig);
  }

  private ImmutableSegment loadNativeIndexSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Map<String, String> propertiesMap = new HashMap<>();
    FSTType fstType = FSTType.NATIVE;
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    Map<String, Map<String, String>> columnPropertiesParentMap = new HashMap<>();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    indexLoadingConfig.setFSTIndexType(fstType);
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    columnPropertiesParentMap.put(DOMAIN_NAMES_COL_NATIVE, propertiesMap);
    indexLoadingConfig.setColumnProperties(columnPropertiesParentMap);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_NATIVE), indexLoadingConfig);
  }

  private void testSelectionResults(String query, int expectedResultSize, List<Serializable[]> expectedResults) {
    Operator<IntermediateResultsBlock> operator = getOperatorForSqlQuery(query);
    IntermediateResultsBlock operatorResult = operator.nextBlock();
    List<Object[]> resultset = (List<Object[]>) operatorResult.getSelectionResult();
    Assert.assertNotNull(resultset);
    Assert.assertEquals(resultset.size(), expectedResultSize);
    if (expectedResults != null) {
      for (int i = 0; i < expectedResultSize; i++) {
        Object[] actualRow = resultset.get(i);
        Object[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow.length, expectedRow.length);
        for (int j = 0; j < actualRow.length; j++) {
          Object actualColValue = actualRow[j];
          Object expectedColValue = expectedRow[j];
          Assert.assertEquals(actualColValue, expectedColValue);
        }
      }
    }
  }

  @Test
  public void testFSTBasedRegexLike() {
    // Select queries on col with FST + inverted index.
    String query = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'www.domain1%') LIMIT 50000";
    _indexSegment = _luceneSegment;
    _indexSegments = Arrays.asList(_indexSegment);
    testSelectionResults(query, 256, null);

    _indexSegment = _nativeIndexSegment;
    _indexSegments = Arrays.asList(_nativeIndexSegment);
    query = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_NATIVE, 'www.domain1.*') LIMIT 50000";
    testSelectionResults(query, 256, null);
  }
}
