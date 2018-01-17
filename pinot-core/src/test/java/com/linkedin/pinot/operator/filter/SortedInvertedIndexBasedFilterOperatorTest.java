package com.linkedin.pinot.operator.filter;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.operator.filter.AndOperator;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.SortedInvertedIndexBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.predicate.BaseRawValueBasedPredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.InPredicateEvaluatorFactory;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory;
import com.linkedin.pinot.core.realtime.RealtimeFileBasedReaderTest;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.realtime.RealtimeSegmentTest;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import com.linkedin.pinot.core.realtime.impl.dictionary.IntOnHeapMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.realtime.impl.kafka.RealtimeSegmentImplTest;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.segments.v1.creator.BitmapInvertedIndexTest;
import com.linkedin.pinot.segments.v1.creator.DictionariesTest;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


public class SortedInvertedIndexBasedFilterOperatorTest {

  private static final String AVRO_DATA_PATH = "data/test_sample_data.avro";
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), SortedInvertedIndexBasedFilterOperatorTest.class.getSimpleName());
  protected static final String TABLE_NAME = "testTable";
  private static final Set<String> INVERTED_INDEX_COLUMNS = new HashSet<String>(3) {
    {
      add("time_day");            // INT, cardinality 1
//      add("column10");            // STRING, cardinality 27
//      add("met_impressionCount"); // LONG, cardinality 21
    }
  };


  private static String filePath;
  private static Map<String, FieldSpec.FieldType> fieldTypeMap;
  private static Schema schema;
  private static IndexSegment segmentWithInvIdx;
  private static RealtimeSegment segmentWithoutInvIdx;

  private final Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();
  protected final List<IndexSegment> _indexSegments = new ArrayList<>();

  private File _avroFile;
  private File _segmentDirectory;
//  private AdminApiApplication _adminApiApplication;
//  protected WebTarget _webTarget;

  private static final String COLUMN_NAME = "count";
  private IndexSegment indexSegment;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(anyString())).thenAnswer(new Answer<TableDataManager>() {
      @SuppressWarnings("SuspiciousMethodCalls")
      @Override
      public TableDataManager answer(InvocationOnMock invocation) throws Throwable {
        return _tableDataManagerMap.get(invocation.getArguments()[0]);
      }
    });
    when(instanceDataManager.getTableDataManagers()).thenReturn(_tableDataManagerMap.values());

//    // Mock the server instance
//    ServerInstance serverInstance = mock(ServerInstance.class);
//    when(serverInstance.getInstanceDataManager()).thenReturn(instanceDataManager);

    // Add the default table and segment
//    addTable(TABLE_NAME);
//    indexSegment = setUpSegment("default");
//
//    _adminApiApplication = new AdminApiApplication(serverInstance);
//    _adminApiApplication.start(CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
//    _webTarget = ClientBuilder.newClient().target(_adminApiApplication.getBaseUri());



//    filePath = RealtimeFileBasedReaderTest.class.getClassLoader().getResource(AVRO_DATA_PATH).getFile();
//    fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
//    fieldTypeMap.put("column1", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column2", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column3", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column4", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column5", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column6", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column7", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column8", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column9", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column10", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("weeksSinceEpochSunday", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("daysSinceEpoch", FieldSpec.FieldType.DIMENSION);
//    fieldTypeMap.put("column13", FieldSpec.FieldType.TIME);
//    fieldTypeMap.put("count", FieldSpec.FieldType.METRIC);
//    schema = SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);
//
//    StreamProviderConfig config = new FileBasedStreamProviderConfig(FileFormat.AVRO, filePath, schema);
////    System.out.println(config);
//    StreamProvider provider = new FileBasedStreamProviderImpl();
//    final String tableName = RealtimeSegmentTest.class.getSimpleName() + ".noTable";
//    provider.init(config, tableName, new ServerMetrics(new MetricsRegistry()));
//
//    List<String> invertedIdxCols = new ArrayList<>();
//    invertedIdxCols.add("count");
//    RealtimeSegmentDataManager segmentDataManager = mock(RealtimeSegmentDataManager.class);
//    when(segmentDataManager.getSegmentName()).thenReturn("noSegment");
//    when(segmentDataManager.getTableName()).thenReturn(tableName);
//    when(segmentDataManager.getInvertedIndexColumns()).thenReturn(invertedIdxCols);
//    when(segmentDataManager.getNoDictionaryColumns()).thenReturn(new ArrayList<String>());
//    when(segmentDataManager.getSchema()).thenReturn(schema);
//    when(segmentDataManager.getMemoryManager()).thenReturn(new DirectMemoryManager("noSegment"));
//    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
//    when(statsHistory.getEstimatedAvgColSize(any(String.class))).thenReturn(32);
//    when(statsHistory.getEstimatedCardinality(any(String.class))).thenReturn(200);
//    when(segmentDataManager.getStatsHistory()).thenReturn(statsHistory);
//
//    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
//    when(indexLoadingConfig.getRealtimeAvgMultiValueCount()).thenReturn(2);
//    segmentWithInvIdx = new RealtimeSegmentImpl(new ServerMetrics(new MetricsRegistry()), segmentDataManager,
//        indexLoadingConfig, 100000, AVRO_DATA_PATH);
//    segmentWithoutInvIdx = RealtimeSegmentImplTest.createRealtimeSegmentImpl(schema, 100000, tableName, "noSegment",
//        AVRO_DATA_PATH, new ServerMetrics(new MetricsRegistry()));
//    GenericRow row = provider.next(new GenericRow());
//    while (row != null) {
//      segmentWithInvIdx.index(row);
////      segmentWithoutInvIdx.index(row);
//      row = GenericRow.createOrReuseRow(row);
//      row = provider.next(row);
//    }
//    provider.shutdown();
  }

  @Test
  public void test() throws Exception {

    FileUtils.deleteQuietly(INDEX_DIR);
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "myTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());


//    final String filePath =
//        TestUtils.getFileFromResourceUrl(DictionariesTest.class.getClassLoader().getResource(AVRO_DATA_PATH));
////    final SegmentGeneratorConfig config =
////        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
////            TimeUnit.DAYS, "test");
//
//
//    SegmentGeneratorConfig config =
//        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, TABLE_NAME);
//    config.setSegmentNamePostfix("default");
//    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
//    driver.init(config);
//    driver.build();
//
//    IndexSegment indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);

//    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 28};
//    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
//
//    List<BaseFilterOperator> operators = new ArrayList<>();
//    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds1));
//    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds2));


//    rangeStart = 2;
//    rangeEnd = 5;
//    ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
//    RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
//    PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);



    DataSource ds = mock(DataSource.class);
    Pairs.IntPair intPair = new Pairs.IntPair(0, 9);

    SortedIndexReader invertedIndexReader = mock(SortedIndexReader.class); //mock(SortedIndexReader.class);//mock(SortedIndexReader.class);
    doReturn(invertedIndexReader).when(ds).getInvertedIndex();

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    doReturn(dataSourceMetadata).when(ds).getDataSourceMetadata();
    doReturn("Test").when(ds).getOperatorName();
    doReturn(FieldSpec.DataType.INT).when(dataSourceMetadata).getDataType();
    MutableDictionary dictionary = spy(IntOnHeapMutableDictionary.class);
    doReturn(true).when(dataSourceMetadata).hasDictionary();
    doReturn(dictionary).when(ds).getDictionary();
    doReturn(10).when(dictionary).length();
    doReturn(0).when(dictionary).getMinVal();
    doReturn(15).when(dictionary).getMaxVal();
//    when(dictionary.inRange(anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean())).thenCallRealMethod();

    doReturn(intPair).when(invertedIndexReader).getDocIds(anyInt());


    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    indexLoadingConfig.setInvertedIndexColumns(INVERTED_INDEX_COLUMNS);
    IndexSegment indexSegment = ColumnarSegmentLoader.load(_segmentDirectory, indexLoadingConfig);


//    DataSource dataSource = indexSegment.getDataSource("time_day");
    List<String> rhs = new ArrayList<String>();
    rhs.add("[*\t\t*]");
    Predicate predicate = new RangePredicate("time_day", rhs);
//    PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, ds);

    PredicateEvaluator predicateEvaluator = spy(PredicateEvaluator.class);
    int[] docIds = {0, 1, 2, 3, 5};
    doReturn(false).when(predicateEvaluator).isExclusive();
    doReturn(docIds).when(predicateEvaluator).getMatchingDictIds();



//    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.INT);


    int startDocId = 0;
    int endDocId = 3;

    SortedInvertedIndexBasedFilterOperator sortedInvertedIndexBasedFilterOperator = new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, ds, startDocId, endDocId);

    sortedInvertedIndexBasedFilterOperator.isResultEmpty();
    BlockDocIdIterator iterator = sortedInvertedIndexBasedFilterOperator.getNextBlock().getBlockDocIdSet().iterator();

    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 28);
    Assert.assertEquals(iterator.next(), Constants.EOF);

//    // Compare the loaded inverted index with the record in avro file
//    try (DataFileStream<GenericRecord> reader = new DataFileStream<>(new FileInputStream(_avroFile),
//        new GenericDatumReader<GenericRecord>())) {
//      // Check the first 1000 records
//      for (int docId = 0; docId < 1000; docId++) {
//        GenericRecord record = reader.next();
//        for (String column : INVERTED_INDEX_COLUMNS) {
//          Object entry = record.get(column);
//          if (entry instanceof Utf8) {
//            entry = entry.toString();
//          }
//          DataSource dataSource = indexSegment.getDataSource(column);
//          Dictionary dictionary = dataSource.getDictionary();
//          InvertedIndexReader invertedIndex = dataSource.getInvertedIndex();
//
//          int dictId = dictionary.indexOf(entry);
//          int size = dictionary.length();
//          if (dataSource.getDataSourceMetadata().isSorted()) {
//            if (size > 1) {
//              System.out.println("~!@~!##");
//            }
//            for (int i = 0; i < size; i++) {
//              Pairs.IntPair minMaxRange = (Pairs.IntPair) invertedIndex.getDocIds(i);
//              int min = minMaxRange.getLeft();
//              int max = minMaxRange.getRight();
//              if (i == dictId) {
//                Assert.assertTrue(docId >= min && docId < max);
//              } else {
//                Assert.assertTrue(docId < min || docId >= max);
//              }
//            }
//          } else {
//            if (size > 1) {
//              System.out.println("~!@~!##2");
//            }
//            for (int i = 0; i < size; i++) {
//              ImmutableRoaringBitmap immutableRoaringBitmap = (ImmutableRoaringBitmap) invertedIndex.getDocIds(i);
//              if (i == dictId) {
//                Assert.assertTrue(immutableRoaringBitmap.contains(docId));
//              } else {
//                Assert.assertFalse(immutableRoaringBitmap.contains(docId));
//              }
//            }
//          }
//        }
//      }
//    }







//    DataSource dataSource = indexSegment.getDataSource(COLUMN_NAME);
//
//
//    List<String> rhs = new ArrayList<String>();
//    rhs.add("[*\t\t*]");
//    Predicate predicate = new RangePredicate(COLUMN_NAME, rhs);
//    PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource);
//
//
////    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.INT);
//
//
//    int startDocId = 0;
//    int endDocId = 3;
//
//    SortedInvertedIndexBasedFilterOperator sortedInvertedIndexBasedFilterOperator = new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
//
//    sortedInvertedIndexBasedFilterOperator.isResultEmpty();
//    BlockDocIdIterator iterator = sortedInvertedIndexBasedFilterOperator.getNextBlock().getBlockDocIdSet().iterator();
//
//    Assert.assertEquals(iterator.next(), 3);
//    Assert.assertEquals(iterator.next(), 28);
//    Assert.assertEquals(iterator.next(), Constants.EOF);
  }

  private PredicateEvaluator buildRangePredicate(String rangeString, FieldSpec.DataType dataType) {
    RangePredicate predicate = new RangePredicate(COLUMN_NAME, Collections.singletonList(rangeString));
    return RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, dataType);
  }

  protected IndexSegment setUpSegment(String segmentNamePostfix) throws Exception {
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, TABLE_NAME);
    config.setSegmentNamePostfix(segmentNamePostfix);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    IndexSegment indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);
    _indexSegments.add(indexSegment);
    _tableDataManagerMap.get(TABLE_NAME).addSegment(indexSegment);
    return indexSegment;
  }

  protected void addTable(String tableName) {
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableDataManagerType()).thenReturn("offline");
    when(tableDataManagerConfig.getTableName()).thenReturn(tableName);
    when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    @SuppressWarnings("unchecked")
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, "testInstance",
            mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class));
    tableDataManager.start();
    _tableDataManagerMap.put(tableName, tableDataManager);
  }

//  private ImmutableDictionaryReader createReader(int rangeStart, int rangeEnd) {
//    ImmutableDictionaryReader reader = mock(ImmutableDictionaryReader.class);
//    when(reader.insertionIndexOf("lower")).thenReturn(rangeStart);
//    when(reader.insertionIndexOf("upper")).thenReturn(rangeEnd);
//    when(reader.length()).thenReturn(DICT_LEN);
//    return reader;
//  }
//
//  private RangePredicate createPredicate(int lower, boolean inclLower, int upper, boolean inclUpper) {
//    RangePredicate predicate = mock(RangePredicate.class);
//    when(predicate.includeLowerBoundary()).thenReturn(inclLower);
//    when(predicate.includeUpperBoundary()).thenReturn(inclUpper);
//    String lowerStr = "lower";
//    if (lower == 0) {
//      lowerStr = "*";
//    }
//    String upperStr = "upper";
//    if (upper == DICT_LEN - 1) {
//      upperStr = "*";
//    }
//    when(predicate.getLowerBoundary()).thenReturn(lowerStr);
//    when(predicate.getUpperBoundary()).thenReturn(upperStr);
//    return predicate;
//  }

  @Test
  public void test2() {
    System.out.println("Hello World!");
  }
}
