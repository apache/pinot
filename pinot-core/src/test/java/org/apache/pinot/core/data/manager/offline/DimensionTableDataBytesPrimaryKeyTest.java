package org.apache.pinot.core.data.manager.offline;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderTest;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;


// Tests primary key lookups for BYTES work
public class DimensionTableDataBytesPrimaryKeyTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), LoaderTest.class.getName());
  private static final String RAW_TABLE_NAME = "dimAsset";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String CSV_DATA_PATH = "data/dimAsset.csv";
  private static final String SCHEMA_PATH = "data/dimAsset_schema.json";
  private static final String TABLE_CONFIG_PATH = "data/dimAsset_config.json";
  private static final SegmentOperationsThrottlerSet SEGMENT_OPERATIONS_THROTTLER = new SegmentOperationsThrottlerSet(
      new SegmentOperationsThrottler(1, 2, true),
      new SegmentOperationsThrottler(1, 2, true),
      new SegmentOperationsThrottler(1, 2, true),
      new SegmentOperationsThrottler(1, 2, true));

  private File _indexDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));

    // prepare segment data
    URL dataPathUrl = getClass().getClassLoader().getResource(CSV_DATA_PATH);
    URL schemaPathUrl = getClass().getClassLoader().getResource(SCHEMA_PATH);
    URL configPathUrl = getClass().getClassLoader().getResource(TABLE_CONFIG_PATH);
    assertNotNull(dataPathUrl);
    assertNotNull(schemaPathUrl);
    assertNotNull(configPathUrl);
    File csvFile = new File(dataPathUrl.getFile());
    TableConfig tableConfig = createTableConfig(new File(configPathUrl.getFile()));
    Schema schema = createSchema(new File(schemaPathUrl.getFile()));

    // create segment
    File tableDataDir = new File(TEMP_DIR, OFFLINE_TABLE_NAME);

    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfig(csvFile, FileFormat.CSV, tableDataDir, RAW_TABLE_NAME, tableConfig,
            schema);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    String segmentName = driver.getSegmentName();
    _indexDir = new File(tableDataDir, segmentName);
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(_indexDir);
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private TableConfig getTableConfig(boolean disablePreload) {
    DimensionTableConfig dimensionTableConfig = new DimensionTableConfig(disablePreload, false);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("dimAsset")
        .setDimensionTableConfig(dimensionTableConfig)
        .build();
  }

  private Schema getSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("dimAsset")
        .addSingleValueDimension("assetId", FieldSpec.DataType.BYTES)
        .addSingleValueDimension("assetName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("contentHash", FieldSpec.DataType.BYTES)
        .setPrimaryKeyColumns(List.of("assetId"))
        .build();
  }

  private DimensionTableDataManager makeTableDataManager(TableConfig tableConfig, Schema schema)
      throws IOException {
    return makeTableDataManager(tableConfig, schema, mock(ZkHelixPropertyStore.class));
  }

  private DimensionTableDataManager makeTableDataManager(TableConfig tableConfig, Schema schema,
      ZkHelixPropertyStore<ZNRecord> propertyStoreMock)
      throws JsonProcessingException {
    HelixManager helixManager = mock(HelixManager.class);
    when(propertyStoreMock.get("/CONFIGS/TABLE/dimAsset_OFFLINE", null, AccessOption.PERSISTENT)).thenReturn(
        TableConfigSerDeUtils.toZNRecord(tableConfig));
    when(propertyStoreMock.get("/SCHEMAS/dimAsset", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaSerDeUtils.toZNRecord(schema));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStoreMock);
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    DimensionTableDataManager tableDataManager =
        DimensionTableDataManager.createInstanceByTableName(OFFLINE_TABLE_NAME);
    tableDataManager.init(instanceDataManagerConfig, helixManager, new SegmentLocks(), tableConfig, schema,
        new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null, SEGMENT_OPERATIONS_THROTTLER,
        false, mock(ServerReloadJobStatusCache.class));
    tableDataManager.start();
    return tableDataManager;
  }

  @Test
  public void testLookupForFastLookupDimensionTable()
      throws Exception {
    TableConfig tableConfig = getTableConfig(false);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);

    // try fetching data BEFORE loading segment
    byte[] rawBytes = BytesUtils.toBytes("550e8400e29b41d4a716446655440000"); // from hex string
    byte[] contentHashBytes = BytesUtils.toBytes("5eb63bbbe01eeed093cb22bb8f5acdc3");
    ByteArray keyValue = new ByteArray(rawBytes);
    PrimaryKey key = new PrimaryKey(new Object[]{keyValue});
    assertFalse(tableDataManager.containsKey(key));
    assertNull(tableDataManager.lookupRow(key));
    assertNull(tableDataManager.lookupValue(key, "assetId"));
    assertNull(tableDataManager.lookupValue(key, "assetName"));
    assertNull(tableDataManager.lookupValue(key, "contentHash"));
    assertNull(tableDataManager.lookupValues(key, new String[]{"assetId", "assetName","contentHash"}));

    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));

    // Confirm table is loaded and available for lookup
    assertTrue(tableDataManager.containsKey(key));
    GenericRow row = tableDataManager.lookupRow(key);
    assertNotNull(row);
    assertEquals(row.getFieldToValueMap().size(), 3);
    assertEquals(row.getValue("assetId"), keyValue);
    assertEquals(row.getValue("assetName"), "logo-final-v2.svg");
    assertEquals(row.getValue("contentHash"), contentHashBytes);
    assertEquals(tableDataManager.lookupValue(key, "assetId"), keyValue);
    assertEquals(tableDataManager.lookupValue(key, "assetName"), "logo-final-v2.svg");
    assertEquals(tableDataManager.lookupValue(key, "contentHash"), contentHashBytes);
    Object[] values = tableDataManager.lookupValues(key, new String[]{"assetId", "assetName", "contentHash"});
    assertNotNull(values);
    assertEquals(values.length, 3);
    assertEquals(values[0], keyValue);
    assertEquals(values[1], "logo-final-v2.svg");
    assertEquals(values[2], contentHashBytes);
  }

  @Test
  public void testLookupForMemoryOptimizedDimensionTable()
      throws Exception {
    TableConfig tableConfig = getTableConfig(true);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);

    // try fetching data BEFORE loading segment
    byte[] rawBytes = BytesUtils.toBytes("550e8400e29b41d4a716446655440000"); // from hex string
    byte[] contentHashBytes = BytesUtils.toBytes("5eb63bbbe01eeed093cb22bb8f5acdc3");
    ByteArray keyValue = new ByteArray(rawBytes);
    PrimaryKey key = new PrimaryKey(new Object[]{keyValue});
    assertFalse(tableDataManager.containsKey(key));
    assertNull(tableDataManager.lookupRow(key));
    assertNull(tableDataManager.lookupValue(key, "assetId"));
    assertNull(tableDataManager.lookupValue(key, "assetName"));
    assertNull(tableDataManager.lookupValue(key, "contentHash"));
    assertNull(tableDataManager.lookupValues(key, new String[]{"assetId", "assetName","contentHash"}));

    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));

    // Confirm table is loaded and available for lookup
    assertTrue(tableDataManager.containsKey(key));
    GenericRow row = tableDataManager.lookupRow(key);
    assertNotNull(row, "Should return response after segment load");
    assertEquals(row.getFieldToValueMap().size(), 3);
    assertEquals(row.getValue("assetName"), "logo-final-v2.svg");
    assertEquals(row.getValue("contentHash"), contentHashBytes);
    assertEquals(tableDataManager.lookupValue(key, "assetName"), "logo-final-v2.svg");
    assertEquals(tableDataManager.lookupValue(key, "contentHash"), contentHashBytes);
    Object[] values = tableDataManager.lookupValues(key, new String[]{"assetId", "assetName", "contentHash"});
    assertNotNull(values);
    assertEquals(values.length, 3);
    assertEquals(values[1], "logo-final-v2.svg");
    assertEquals(values[2], contentHashBytes);
  }

  protected static Schema createSchema(File schemaFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(schemaFile);
    Assert.assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, Schema.class);
  }

  protected static TableConfig createTableConfig(File tableConfigFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(tableConfigFile);
    Assert.assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
  }
}
