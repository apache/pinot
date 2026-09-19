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
package org.apache.pinot.controller.util;

import com.google.common.collect.HashBiMap;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/// Tests filesystem-independent loading of page cache warmup queries.
public class PageCacheWarmupControllerExecutorTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = RAW_TABLE_NAME + "_OFFLINE";
  private static final byte[] QUERIES_JSON =
      "[\"SELECT COUNT(*) FROM testTable\"]".getBytes(StandardCharsets.UTF_8);

  @Test(dataProvider = "s3Schemes")
  public void testLoadsMostRecentlyModifiedRemoteQueryFile(String scheme)
      throws Exception {
    String dataDir = scheme + "://bucket/page-cache-warmup";
    URI tableDirUri = URI.create(dataDir + "/" + TABLE_NAME_WITH_TYPE);
    String tablePath = "/page-cache-warmup/" + TABLE_NAME_WITH_TYPE;
    URI oldQueryFileUri = URI.create(tableDirUri + "/queries-old");
    String latestQueryFilePath = tablePath + "/queries-%41.json";
    URI latestQueryFileUri = new URI(scheme, "bucket", latestQueryFilePath, null, null);
    URI directoryUri = URI.create(tableDirUri + "/archive");

    PinotFS pinotFS = mock(PinotFS.class);
    when(pinotFS.listFilesWithMetadata(tableDirUri, false)).thenReturn(List.of(
        fileMetadata(oldQueryFileUri.toString(), 100L, false),
        fileMetadata(directoryUri.toString(), 300L, true),
        fileMetadata(scheme + "://bucket" + latestQueryFilePath, 0L, false)));
    when(pinotFS.lastModified(latestQueryFileUri)).thenReturn(200L);
    when(pinotFS.open(latestQueryFileUri)).thenReturn(new ByteArrayInputStream(QUERIES_JSON));

    PageCacheWarmupControllerExecutor executor = createExecutor(createResourceManager(), dataDir);
    try (MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      pinotFSFactory.when(() -> PinotFSFactory.create(scheme)).thenReturn(pinotFS);

      executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

      verify(pinotFS).listFilesWithMetadata(tableDirUri, false);
      verify(pinotFS, never()).exists(tableDirUri);
      verify(pinotFS).open(latestQueryFileUri);
      verify(pinotFS, never()).lastModified(directoryUri);
      verify(pinotFS, never()).open(oldQueryFileUri);
    } finally {
      executor.shutdown();
    }
  }

  @DataProvider(name = "s3Schemes")
  public Object[][] s3Schemes() {
    return new Object[][]{{"s3"}, {"s3a"}};
  }

  @Test
  public void testLoadsPathOnlyHdfsMetadata()
      throws Exception {
    String dataDir = "hdfs://nameservice/page-cache-warmup";
    URI tableDirUri = URI.create(dataDir + "/" + TABLE_NAME_WITH_TYPE);
    String tablePath = "/page-cache-warmup/" + TABLE_NAME_WITH_TYPE;
    String oldQueryFilePath = tablePath + "/queries-old";
    String directoryPath = tablePath + "/archive";
    String latestQueryFilePath = tablePath + "/queries-#?%.json";
    URI oldQueryFileUri = URI.create("hdfs://nameservice" + oldQueryFilePath);
    URI directoryUri = URI.create("hdfs://nameservice" + directoryPath);
    URI latestQueryFileUri = new URI("hdfs", "nameservice", latestQueryFilePath, null, null);

    PinotFS pinotFS = mock(PinotFS.class);
    when(pinotFS.listFilesWithMetadata(tableDirUri, false)).thenReturn(List.of(
        fileMetadata(oldQueryFilePath, 100L, false),
        fileMetadata(directoryPath, 300L, true),
        fileMetadata(latestQueryFilePath, 200L, false)));
    when(pinotFS.open(latestQueryFileUri)).thenReturn(new ByteArrayInputStream(QUERIES_JSON));

    PageCacheWarmupControllerExecutor executor = createExecutor(createResourceManager(), dataDir);
    try (MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      pinotFSFactory.when(() -> PinotFSFactory.create("hdfs")).thenReturn(pinotFS);

      executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

      verify(pinotFS).open(latestQueryFileUri);
      verify(pinotFS, never()).lastModified(directoryUri);
      verify(pinotFS, never()).open(oldQueryFileUri);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testRelativeLocalDataDirectory()
      throws Exception {
    String dataDir = "target/page cache warmup relative";
    URI tableDirUri = new File(dataDir, TABLE_NAME_WITH_TYPE).toURI();
    URI queryFileUri = new File(new File(dataDir, TABLE_NAME_WITH_TYPE), "queries").toURI();
    PinotFS pinotFS = mock(PinotFS.class);
    when(pinotFS.listFilesWithMetadata(tableDirUri, false)).thenReturn(
        List.of(fileMetadata(queryFileUri.toString(), 100L, false)));
    when(pinotFS.open(queryFileUri)).thenReturn(new ByteArrayInputStream(QUERIES_JSON));

    PageCacheWarmupControllerExecutor executor = createExecutor(createResourceManager(), dataDir);
    try (MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      pinotFSFactory.when(() -> PinotFSFactory.create("file")).thenReturn(pinotFS);

      executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

      verify(pinotFS).listFilesWithMetadata(tableDirUri, false);
      verify(pinotFS).open(queryFileUri);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testNoQueryFilesSkipsWarmup()
      throws Exception {
    String dataDir = "s3://bucket/page-cache-warmup";
    URI tableDirUri = URI.create(dataDir + "/" + TABLE_NAME_WITH_TYPE);
    PinotFS pinotFS = mock(PinotFS.class);
    when(pinotFS.listFilesWithMetadata(tableDirUri, false)).thenReturn(List.of());
    PinotHelixResourceManager resourceManager = createResourceManager();

    PageCacheWarmupControllerExecutor executor = createExecutor(resourceManager, dataDir);
    try (MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      pinotFSFactory.when(() -> PinotFSFactory.create("s3")).thenReturn(pinotFS);

      executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

      verify(pinotFS, never()).exists(tableDirUri);
      verify(pinotFS, never()).open(any());
      verify(resourceManager, never()).getServerInstancesForTable(RAW_TABLE_NAME, TableType.OFFLINE);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testListingFailureForExistingDirectoryDoesNotUseFallback()
      throws Exception {
    String dataDir = "s3://bucket/page-cache-warmup";
    URI tableDirUri = URI.create(dataDir + "/" + TABLE_NAME_WITH_TYPE);
    PinotFS pinotFS = mock(PinotFS.class);
    when(pinotFS.listFilesWithMetadata(tableDirUri, false)).thenThrow(new IOException("Listing failed"));
    when(pinotFS.exists(tableDirUri)).thenReturn(true);
    PinotHelixResourceManager resourceManager = createResourceManager();
    ControllerMetrics controllerMetrics = mock(ControllerMetrics.class);

    try (MockedStatic<ControllerMetrics> controllerMetricsFactory = mockStatic(ControllerMetrics.class);
        MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      controllerMetricsFactory.when(ControllerMetrics::get).thenReturn(controllerMetrics);
      pinotFSFactory.when(() -> PinotFSFactory.create("s3")).thenReturn(pinotFS);
      PageCacheWarmupControllerExecutor executor = createExecutor(resourceManager, dataDir);
      try {
        executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

        verify(pinotFS, never()).open(any());
        verify(resourceManager, never()).getServerInstancesForTable(RAW_TABLE_NAME, TableType.OFFLINE);
        verify(controllerMetrics).addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUEST_ERRORS, 1L);
      } finally {
        executor.shutdown();
      }
    }
  }

  @Test
  public void testEncodesRemoteTableDirectoryName()
      throws Exception {
    String rawTableName = "test#?%2F+Table";
    String tableNameWithType = rawTableName + "_OFFLINE";
    String dataDir = "s3://bucket/page-cache-warmup";
    URI tableDirUri =
        URI.create(dataDir + "/test%23%3F%252F%2BTable_OFFLINE");
    PinotFS pinotFS = mock(PinotFS.class);
    when(pinotFS.listFilesWithMetadata(tableDirUri, false)).thenReturn(List.of());

    PageCacheWarmupControllerExecutor executor = createExecutor(createResourceManager(rawTableName), dataDir);
    try (MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      pinotFSFactory.when(() -> PinotFSFactory.create("s3")).thenReturn(pinotFS);

      executor.triggerPageCacheWarmup(tableNameWithType, List.of("segment"));

      verify(pinotFS).listFilesWithMetadata(tableDirUri, false);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testMalformedRemoteDataDirectoryDoesNotFallBackToLocal()
      throws Exception {
    String dataDir = "s3://bucket/page cache";
    PinotHelixResourceManager resourceManager = createResourceManager();
    PageCacheWarmupControllerExecutor executor = createExecutor(resourceManager, dataDir);
    try (MockedStatic<PinotFSFactory> pinotFSFactory = mockStatic(PinotFSFactory.class)) {
      executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

      pinotFSFactory.verifyNoInteractions();
      verify(resourceManager, never()).getServerInstancesForTable(RAW_TABLE_NAME, TableType.OFFLINE);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testMissingLocalQueryDirectorySkipsWarmup()
      throws Exception {
    Path dataDir = Files.createTempDirectory("page-cache-warmup");
    PinotHelixResourceManager resourceManager = createResourceManager();
    ControllerMetrics controllerMetrics = mock(ControllerMetrics.class);

    try (MockedStatic<ControllerMetrics> controllerMetricsFactory = mockStatic(ControllerMetrics.class)) {
      controllerMetricsFactory.when(ControllerMetrics::get).thenReturn(controllerMetrics);
      PageCacheWarmupControllerExecutor executor = createExecutor(resourceManager, dataDir.toString());
      try {
        executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

        verify(resourceManager, never()).getServerInstancesForTable(RAW_TABLE_NAME, TableType.OFFLINE);
        verify(controllerMetrics).addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS, 1L);
        verify(controllerMetrics, never())
            .addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUEST_ERRORS, 1L);
      } finally {
        executor.shutdown();
      }
    } finally {
      FileUtils.deleteDirectory(dataDir.toFile());
    }
  }

  @Test
  public void testRealLocalPinotFSSelectsLatestFile()
      throws Exception {
    Path dataDir = Files.createTempDirectory("page-cache-warmup");
    Path tableDir = Files.createDirectories(dataDir.resolve(TABLE_NAME_WITH_TYPE));
    Path oldQueryFile = tableDir.resolve("queries-old");
    Path latestQueryFile = tableDir.resolve("queries-#?%.json");
    Files.writeString(oldQueryFile, "not-json", StandardCharsets.UTF_8);
    Files.write(latestQueryFile, QUERIES_JSON);
    Files.setLastModifiedTime(oldQueryFile, FileTime.fromMillis(100L));
    Files.setLastModifiedTime(latestQueryFile, FileTime.fromMillis(200L));

    PinotHelixResourceManager resourceManager = createResourceManager();
    PageCacheWarmupControllerExecutor executor = createExecutor(resourceManager, dataDir.toString());
    try {
      executor.triggerPageCacheWarmup(TABLE_NAME_WITH_TYPE, List.of("segment"));

      verify(resourceManager).getServerInstancesForTable(RAW_TABLE_NAME, TableType.OFFLINE);
    } finally {
      executor.shutdown();
      FileUtils.deleteDirectory(dataDir.toFile());
    }
  }

  private static PinotHelixResourceManager createResourceManager()
      throws InvalidConfigException {
    return createResourceManager(RAW_TABLE_NAME);
  }

  private static PinotHelixResourceManager createResourceManager(String rawTableName)
      throws InvalidConfigException {
    PageCacheWarmupConfig.Spec refreshSpec = new PageCacheWarmupConfig.Spec(true, null, null, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).build();
    tableConfig.setPageCacheWarmupConfig(new PageCacheWarmupConfig(null, refreshSpec));

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getOfflineTableConfig(rawTableName)).thenReturn(tableConfig);
    when(resourceManager.getServerInstancesForTable(rawTableName, TableType.OFFLINE)).thenReturn(List.of());
    when(resourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(HashBiMap.create());
    return resourceManager;
  }

  private static PageCacheWarmupControllerExecutor createExecutor(PinotHelixResourceManager resourceManager,
      String dataDir) {
    ControllerConf controllerConf =
        new ControllerConf(Map.of(ControllerConf.CONFIG_OF_PAGE_CACHE_WARMUP_QUERIES_DATA_DIR, dataDir));
    return new PageCacheWarmupControllerExecutor(resourceManager, controllerConf);
  }

  private static FileMetadata fileMetadata(String filePath, long lastModifiedTime, boolean isDirectory) {
    return new FileMetadata.Builder().setFilePath(filePath).setLastModifiedTime(lastModifiedTime)
        .setIsDirectory(isDirectory).build();
  }
}
