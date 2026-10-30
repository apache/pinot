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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies dimension-table deletion and server cleanup using one server in the shared custom-data cluster.
@Test(suiteName = "CustomClusterIntegrationTest")
public class DimensionTableIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "dimensionTableDeletion";
  private static final String SERVER_TENANT = "dimensionTableDeletionTenant";
  private static final String SERVER_TAG = TagNameUtils.getOfflineTagForTenant(SERVER_TENANT);
  private static final String LONG_COL = "longCol";
  private static final String INT_COL = "intCol";
  private String _taggedServerInstance;
  private long _segmentCreationTime;
  private File _controllerSegmentFile;

  @Test
  public void testDelete()
      throws Exception {
    JsonNode node = postQuery("select count(*) from " + getTableName());
    assertNoError(node);
    assertEquals(node.get("resultTable").get("rows").get(0).get(0).asInt(), getCountStarResult());

    DimensionTableDataManager tableDataManager =
        DimensionTableDataManager.getInstanceByTableName(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()));
    assertNotNull(tableDataManager);
    List<SegmentDataManager> segments = tableDataManager.acquireAllSegments();
    try {
      assertEquals(segments.size(), 2);
      for (SegmentDataManager segment : segments) {
        assertEquals(segment.getSegment().getSegmentMetadata().getIndexCreationTime(), _segmentCreationTime);
      }
    } finally {
      for (SegmentDataManager segment : segments) {
        tableDataManager.releaseSegment(segment);
      }
    }
    assertEquals(tableDataManager.lookupValue(new PrimaryKey(new Object[]{0L}), INT_COL), 0);
    assertEquals(tableDataManager.lookupValue(new PrimaryKey(new Object[]{499L}), INT_COL), 499);
    assertTrue(_controllerSegmentFile.isFile(), "Full upload must create a controller-managed segment file");

    dropOfflineTable(getTableName(), "-1d");

    waitForTableToUnload();
    TestUtils.waitForCondition(aVoid -> !_controllerSegmentFile.exists(), 60_000L,
        "Failed to delete controller-managed segment file: " + _controllerSegmentFile);

    assertNull(
        DimensionTableDataManager.getInstanceByTableName(TableNameBuilder.OFFLINE.tableNameWithType(getTableName())));
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(LONG_COL, DataType.LONG)
        .addSingleValueDimension(INT_COL, DataType.INT)
        .setPrimaryKeyColumns(List.of(LONG_COL))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(LONG_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(INT_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null)));

    ArrayList<File> files = new ArrayList<>();

    for (int fi = 0; fi < 2; fi++) {
      File file = new File(_tempDir, "data" + fi + ".avro");
      try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        writer.create(avroSchema, file);
        for (int i = 0; i < getCountStarResult() / 2; i++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(LONG_COL, i);
          record.put(INT_COL, i);
          writer.append(record);
        }
      }
      files.add(file);
    }
    return files;
  }

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Override
  protected void setUpTable()
      throws Exception {
    // Dimension managers are JVM-wide singletons. Preserve the original one-server placement without changing
    // existing server tags or starting another server in the shared cluster.
    assertTrue(getSharedHelixResourceManager().getHelixAdmin()
        .getInstancesInClusterWithTag(getHelixClusterName(), SERVER_TAG).isEmpty());
    String serverInstance = getSharedServerStarters().get(0).getInstanceId();
    getSharedHelixResourceManager().getHelixAdmin()
        .addInstanceTag(getHelixClusterName(), serverInstance, SERVER_TAG);
    _taggedServerInstance = serverInstance;

    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Give duplicate primary keys an equal creation time so loading both segments exercises segment-name ordering.
    _segmentCreationTime = System.currentTimeMillis();
    List<File> segmentTarFiles = new ArrayList<>();
    for (int i = 0; i < avroFiles.size(); i++) {
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setFormat(FileFormat.AVRO);
      config.setInputFilePath(avroFiles.get(i).getPath());
      config.setOutDir(_segmentDir.getPath());
      config.setTableName(tableConfig.getTableName());
      config.setSegmentNamePostfix(i + " %");
      config.setCreationTime(Long.toString(_segmentCreationTime));

      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();

      String segmentName = driver.getSegmentName();
      File segmentTarFile = new File(_tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
      TarCompressionUtils.createCompressedTarFile(new File(_segmentDir, segmentName), segmentTarFile);
      segmentTarFiles.add(segmentTarFile);
      if (i == 0) {
        // Match LocalPinotFS decoding, including '+' for the space in the segment name.
        URI segmentUri = URIUtils.getUri(getSharedHelixResourceManager().getDataDir(), getTableName(),
            URIUtils.encode(segmentName));
        _controllerSegmentFile = new File(URIUtils.decode(segmentUri.getRawPath()));
      }
    }

    URI uploadSegmentHttpURI = URI.create(getOrCreateAdminClient().getSegmentUploadUrl());
    // Preserve parallel segment uploads while exercising both upload paths without a wall-clock choice.
    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      Future<Integer> fullUploadStatus = executor.submit(() -> {
        try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
          File fullUpload = segmentTarFiles.get(0);
          return client.uploadSegment(uploadSegmentHttpURI, fullUpload.getName(), fullUpload,
              getSegmentUploadAuthHeaders(), getTableName(), TableType.OFFLINE).getStatusCode();
        }
      });
      Future<Integer> metadataUploadStatus = executor.submit(() -> {
        try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
          File metadataUpload = segmentTarFiles.get(1);
          List<Header> headers = new ArrayList<>(List.of(
              new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI,
                  "file://" + metadataUpload.getParentFile().getAbsolutePath() + "/"
                      + URIUtils.encode(metadataUpload.getName())),
              new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
                  FileUploadDownloadClient.FileUploadType.METADATA.toString())));
          headers.addAll(getSegmentUploadAuthHeaders());
          List<NameValuePair> parameters = List.of(
              new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, getTableName()),
              new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, TableType.OFFLINE.name()));
          return client.uploadSegmentMetadata(uploadSegmentHttpURI, metadataUpload.getName(), metadataUpload, headers,
              parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode();
        }
      });
      assertEquals((int) fullUploadStatus.get(), HttpStatus.SC_OK);
      assertEquals((int) metadataUploadStatus.get(), HttpStatus.SC_OK);
    }
  }

  private void waitForTableToUnload() {
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TestUtils.waitForCondition(aVoid -> getSharedHelixResourceManager().getTableExternalView(tableNameWithType) == null,
        60_000L, "Failed to clean up the external view for table: " + tableNameWithType);
    TestUtils.waitForCondition(aVoid -> getSharedServerStarters().stream().allMatch(server ->
        server.getServerInstance().getInstanceDataManager().getTableDataManager(tableNameWithType) == null)
        && DimensionTableDataManager.getInstanceByTableName(tableNameWithType) == null,
        60_000L, "Failed to unload dimension table from the shared servers: " + tableNameWithType);
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws IOException {
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    if (getSharedHelixResourceManager().getTableConfig(tableNameWithType) != null) {
      dropOfflineTable(getTableName(), "-1d");
    }
    // Keep the tenant tag and local files if unloading fails, so cleanup cannot disturb an active assignment.
    waitForTableToUnload();
    try {
      try {
        if (getSharedHelixResourceManager().getSchema(getTableName()) != null) {
          deleteSchema(getTableName());
        }
      } finally {
        if (_taggedServerInstance != null) {
          getSharedHelixResourceManager().getHelixAdmin()
              .removeInstanceTag(getHelixClusterName(), _taggedServerInstance, SERVER_TAG);
          _taggedServerInstance = null;
        }
      }
    } finally {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setServerTenant(SERVER_TENANT)
        .setDimensionTableConfig(new DimensionTableConfig(false, false))
        .setIsDimTable(true)
        .build();
  }
}
