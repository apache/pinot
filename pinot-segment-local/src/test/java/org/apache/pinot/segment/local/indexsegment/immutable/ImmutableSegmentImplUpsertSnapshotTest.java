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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.ConcurrentMapPartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ImmutableSegmentImplUpsertSnapshotTest {
  private static final String AVRO_FILE = "data/test_data-mv.avro";
  private static final String SCHEMA = "data/testDataMVSchema.json";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ImmutableSegmentImplTest");

  private SegmentDirectory _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;
  private PinotConfiguration _configuration;
  private TableConfig _tableConfig;
  private Schema _schema;

  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private ImmutableSegmentImpl _immutableSegmentImpl;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());
    _configuration = new PinotConfiguration(props);

    _segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(INDEX_DIR.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());

    URL resourceUrl = ImmutableSegmentImpl.class.getClassLoader().getResource(AVRO_FILE);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(false);
    ingestionConfig.setSegmentTimeValueCheck(false);

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableSnapshot(true);

    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
            .setIngestionConfig(ingestionConfig).setUpsertConfig(upsertConfig).build();

    resourceUrl = ImmutableSegmentImpl.class.getClassLoader().getResource(SCHEMA);
    _schema = Schema.fromFile(new File(resourceUrl.getFile()));

    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(avroFile, INDEX_DIR, "testTable", _tableConfig, _schema);

    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    _segmentMetadata = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(_segmentMetadata.getColumnMetadataMap()).thenReturn(new HashMap<>());
    Mockito.when(_segmentMetadata.getIndexDir()).thenReturn(INDEX_DIR);
    _immutableSegmentImpl = new ImmutableSegmentImpl(_segmentDirectory, _segmentMetadata, new HashMap<>(), null);

    ServerMetrics serverMetrics = Mockito.mock(ServerMetrics.class);
    _partitionUpsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager("testTable_REALTIME", 0, Collections.singletonList("column6"),
            "daysSinceEpoch", HashFunction.NONE, null, true, serverMetrics);

    _immutableSegmentImpl.enableUpsert(_partitionUpsertMetadataManager, new ThreadSafeMutableRoaringBitmap());
  }

  @Test
  public void testPersistValidDocIdsSnapshot() {
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    MutableRoaringBitmap validDocIds = new MutableRoaringBitmap();
    validDocIds.add(docIds1);

    _immutableSegmentImpl.persistValidDocIdsSnapshot(validDocIds);
    assertTrue(new File(SegmentDirectoryPaths.findSegmentDirectory(_segmentMetadata.getIndexDir()),
        V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME).exists());

    MutableRoaringBitmap bitmap = _immutableSegmentImpl.loadValidDocIdsFromSnapshot();
    assertEquals(bitmap.toArray(), docIds1);

    _immutableSegmentImpl.deleteValidDocIdsSnapshot();
    assertFalse(new File(SegmentDirectoryPaths.findSegmentDirectory(_segmentMetadata.getIndexDir()),
        V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME).exists());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
