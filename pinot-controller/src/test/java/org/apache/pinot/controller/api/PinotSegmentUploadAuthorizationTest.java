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
package org.apache.pinot.controller.api;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.constants.SegmentUploadConstants;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "stateless")
public class PinotSegmentUploadAuthorizationTest extends ControllerTest {
  private static final String TABLE_A = "segmentAuthTableA";
  private static final String TABLE_B = "segmentAuthTableB";
  private static final String DATABASE = "segmentAuthDatabase";
  private static final String DATABASE_TABLE_NAME = "segmentAuthDatabaseTable";
  private static final String DATABASE_TABLE = DATABASE + "." + DATABASE_TABLE_NAME;
  private static final String TABLE_A_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_A);
  private static final String TABLE_B_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_B);
  private static final String DATABASE_TABLE_WITH_TYPE =
      TableNameBuilder.OFFLINE.tableNameWithType(DATABASE_TABLE);
  private static final String SEGMENT_A = "segmentA";
  private static final String SEGMENT_B = "segmentB";
  private static final String SEGMENT_DATABASE = "segmentDatabase";

  private File _testDir;
  private File _segmentA;
  private File _segmentB;
  private File _segmentBRefresh;
  private File _segmentDatabase;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.put(ControllerConf.ACCESS_CONTROL_FACTORY_CLASS,
        DestinationAccessControlFactory.class.getName());
    startController(controllerConfig);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    _testDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
    FileUtils.deleteQuietly(_testDir);
    FileUtils.forceMkdir(_testDir);

    addTable(TABLE_A);
    addTable(TABLE_B);
    addTable(DATABASE_TABLE);
    _segmentA = buildSegment(TABLE_A, SEGMENT_A, SEGMENT_A, 1);
    _segmentB = buildSegment(TABLE_B, SEGMENT_B, SEGMENT_B, 1);
    _segmentBRefresh = buildSegment(TABLE_B, SEGMENT_B, SEGMENT_B + "Refresh", 2);
    _segmentDatabase = buildSegment(DATABASE_TABLE, SEGMENT_DATABASE, SEGMENT_DATABASE, 1);
    DestinationAccessControlFactory.setAllowedTables(Set.of(TABLE_A));
  }

  @Test
  public void testDestinationAuthorizationForSegmentUploads()
      throws Exception {
    URI uploadUri = URI.create(getControllerBaseApiUrl() + "/segments");
    URI v2UploadUri = URI.create(getControllerBaseApiUrl() + "/v2/segments");
    URI batchUploadUri = URI.create(getControllerBaseApiUrl() + "/segments/batchUpload");
    AtomicInteger segmentDownloadCount = new AtomicInteger();
    HttpServer segmentServer = startSegmentServer(
        Map.of(SEGMENT_A, _segmentA, SEGMENT_B, _segmentB, SEGMENT_DATABASE, _segmentDatabase),
        segmentDownloadCount);
    File batchMetadataTar = null;
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      batchMetadataTar = createBatchMetadataTar(_segmentDatabase, SEGMENT_DATABASE,
          segmentUrl(segmentServer, SEGMENT_DATABASE));
      File batchMetadataTarForRequest = batchMetadataTar;
      SimpleHttpResponse uploadResponse = uploadMultipart(client, uploadUri, _segmentA, TABLE_A, List.of());
      Assert.assertEquals(uploadResponse.getStatusCode(), Response.Status.OK.getStatusCode());

      SegmentZKMetadata segmentAMetadata =
          _helixResourceManager.getSegmentZKMetadata(TABLE_A_WITH_TYPE, SEGMENT_A);
      Assert.assertNotNull(segmentAMetadata);
      Assert.assertEquals(segmentAMetadata.getRefreshTime(), Long.MIN_VALUE);

      String segmentAUri = segmentUrl(segmentServer, SEGMENT_A);
      int downloadsBeforeInvalidRequest = segmentDownloadCount.get();
      HttpErrorStatusException earlyHeaderMismatch = assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadJson(client, uploadUri, segmentAUri, TABLE_A,
              List.of(new BasicHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER, TABLE_B))));
      Assert.assertTrue(
          earlyHeaderMismatch.getMessage().contains(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER),
          earlyHeaderMismatch.getMessage());
      Assert.assertEquals(segmentDownloadCount.get(), downloadsBeforeInvalidRequest);

      SimpleHttpResponse refreshResponse = uploadJson(client, uploadUri, segmentAUri, TABLE_A, List.of());
      Assert.assertEquals(refreshResponse.getStatusCode(), Response.Status.OK.getStatusCode());
      segmentAMetadata = _helixResourceManager.getSegmentZKMetadata(TABLE_A_WITH_TYPE, SEGMENT_A);
      Assert.assertTrue(segmentAMetadata.getRefreshTime() > 0);

      assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadMultipart(client, v2UploadUri, _segmentA, null, List.of()));
      assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadMultipart(client, v2UploadUri, _segmentA, " ", List.of()));
      assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadMultipart(client, v2UploadUri, _segmentA, "otherDatabase." + TABLE_A,
              List.of(new BasicHeader(CommonConstants.DATABASE, DATABASE))));

      // V2 remains request-bound and keeps the supported promotion behavior when segment metadata names another table.
      Assert.assertEquals(uploadMultipart(client, v2UploadUri, _segmentB, TABLE_A, List.of()).getStatusCode(),
          Response.Status.OK.getStatusCode());
      Assert.assertNotNull(_helixResourceManager.getSegmentMetadataZnRecord(TABLE_A_WITH_TYPE, SEGMENT_B));
      assertNoSegmentState(TABLE_B, TABLE_B_WITH_TYPE, SEGMENT_B);

      assertHttpStatus(Response.Status.FORBIDDEN,
          () -> uploadMultipart(client, v2UploadUri, _segmentA, TABLE_B, List.of()));
      assertNoSegmentState(TABLE_B, TABLE_B_WITH_TYPE, SEGMENT_A);

      assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadMultipart(client, batchUploadUri, _segmentDatabase, TABLE_A,
              List.of(new BasicHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER, TABLE_B))));
      assertNoSegmentState(TABLE_A, TABLE_A_WITH_TYPE, SEGMENT_DATABASE);

      List<Header> batchUploadHeaders = List.of(new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
          FileUploadDownloadClient.FileUploadType.METADATA.toString()));
      HttpErrorStatusException batchMetadataMismatch = assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> client.uploadSegmentMetadata(batchUploadUri, batchMetadataTarForRequest.getName(),
              batchMetadataTarForRequest, batchUploadHeaders, uploadParameters(TABLE_A),
              HttpClient.DEFAULT_SOCKET_TIMEOUT_MS));
      Assert.assertTrue(batchMetadataMismatch.getMessage().contains("segment metadata table name"),
          batchMetadataMismatch.getMessage());
      assertNoSegmentState(TABLE_A, TABLE_A_WITH_TYPE, SEGMENT_DATABASE);

      List<Header> blankTableHeader = List.of(
          new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
              FileUploadDownloadClient.FileUploadType.METADATA.toString()),
          new BasicHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER, " "));
      assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> client.uploadSegmentMetadata(batchUploadUri, batchMetadataTarForRequest.getName(),
              batchMetadataTarForRequest, blankTableHeader, uploadParameters(TABLE_A),
              HttpClient.DEFAULT_SOCKET_TIMEOUT_MS));
      assertNoSegmentState(TABLE_A, TABLE_A_WITH_TYPE, SEGMENT_DATABASE);

      DestinationAccessControlFactory.setAllowedTables(Set.of(TABLE_A, DATABASE_TABLE));
      List<Header> databaseHeaders = List.of(
          new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
              FileUploadDownloadClient.FileUploadType.METADATA.toString()),
          new BasicHeader(CommonConstants.DATABASE, DATABASE));
      Assert.assertEquals(client.uploadSegmentMetadata(batchUploadUri, batchMetadataTar.getName(), batchMetadataTar,
          databaseHeaders, uploadParameters(DATABASE_TABLE_NAME), HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode(),
          Response.Status.OK.getStatusCode());
      Assert.assertNotNull(
          _helixResourceManager.getSegmentMetadataZnRecord(DATABASE_TABLE_WITH_TYPE, SEGMENT_DATABASE));
      Assert.assertNull(_helixResourceManager.getSegmentMetadataZnRecord(
          TableNameBuilder.OFFLINE.tableNameWithType(DATABASE_TABLE_NAME), SEGMENT_DATABASE));
      DestinationAccessControlFactory.setAllowedTables(Set.of(TABLE_A));

      assertHttpStatus(Response.Status.FORBIDDEN, () -> uploadMultipart(
          client, uploadUri, _segmentB, null, List.of()));
      assertNoSegmentState(TABLE_B, TABLE_B_WITH_TYPE, SEGMENT_B);

      assertHttpStatus(Response.Status.FORBIDDEN,
          () -> uploadJson(client, uploadUri, segmentUrl(segmentServer, SEGMENT_B), null, List.of()));
      assertNoSegmentState(TABLE_B, TABLE_B_WITH_TYPE, SEGMENT_B);

      HttpErrorStatusException mismatch = assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadMultipart(client, uploadUri, _segmentB, TABLE_A, List.of()));
      Assert.assertTrue(mismatch.getMessage().contains("segment metadata table name"), mismatch.getMessage());
      assertNoSegmentState(TABLE_B, TABLE_B_WITH_TYPE, SEGMENT_B);

      long segmentARefreshTime = segmentAMetadata.getRefreshTime();
      HttpErrorStatusException headerMismatch = assertHttpStatus(Response.Status.BAD_REQUEST,
          () -> uploadMultipart(client, uploadUri, _segmentA, TABLE_A,
              List.of(new BasicHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER, TABLE_B))));
      Assert.assertTrue(headerMismatch.getMessage().contains(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER),
          headerMismatch.getMessage());
      Assert.assertEquals(_helixResourceManager.getSegmentZKMetadata(TABLE_A_WITH_TYPE, SEGMENT_A).getRefreshTime(),
          segmentARefreshTime);

      DestinationAccessControlFactory.setAllowedTables(Set.of(TABLE_A, TABLE_B));
      Assert.assertEquals(uploadMultipart(client, uploadUri, _segmentB, TABLE_B, List.of()).getStatusCode(),
          Response.Status.OK.getStatusCode());
      DestinationAccessControlFactory.setAllowedTables(Set.of(TABLE_A));

      ZNRecord segmentBZkBefore = _helixResourceManager.getSegmentMetadataZnRecord(TABLE_B_WITH_TYPE, SEGMENT_B);
      Assert.assertNotNull(segmentBZkBefore);
      File segmentBDeepStoreFile = new File(new File(_controllerConfig.getDataDir(), TABLE_B), SEGMENT_B);
      Assert.assertTrue(segmentBDeepStoreFile.isFile());
      byte[] segmentBBytesBefore = Files.readAllBytes(segmentBDeepStoreFile.toPath());
      FileTime segmentBModifiedBefore = Files.getLastModifiedTime(segmentBDeepStoreFile.toPath());
      Assert.assertFalse(Arrays.equals(Files.readAllBytes(_segmentBRefresh.toPath()), segmentBBytesBefore));

      // Omit tableName so the request passes cluster CREATE authorization and reaches the final-table gate.
      assertHttpStatus(Response.Status.FORBIDDEN,
          () -> uploadMultipart(client, uploadUri, _segmentBRefresh, null, List.of()));

      ZNRecord segmentBZkAfter = _helixResourceManager.getSegmentMetadataZnRecord(TABLE_B_WITH_TYPE, SEGMENT_B);
      Assert.assertEquals(segmentBZkAfter.getVersion(), segmentBZkBefore.getVersion());
      Assert.assertEquals(segmentBZkAfter.getSimpleFields(), segmentBZkBefore.getSimpleFields());
      Assert.assertEquals(segmentBZkAfter.getListFields(), segmentBZkBefore.getListFields());
      Assert.assertEquals(segmentBZkAfter.getMapFields(), segmentBZkBefore.getMapFields());
      Assert.assertTrue(Arrays.equals(Files.readAllBytes(segmentBDeepStoreFile.toPath()), segmentBBytesBefore));
      Assert.assertEquals(Files.getLastModifiedTime(segmentBDeepStoreFile.toPath()), segmentBModifiedBefore);
    } finally {
      DestinationAccessControlFactory.setAllowedTables(Set.of(TABLE_A));
      FileUtils.deleteQuietly(batchMetadataTar);
      segmentServer.stop(0);
    }
  }

  private void addTable(String tableName)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension("value", DataType.INT).build();
    _helixResourceManager.addSchema(schema, false, false);
    _helixResourceManager.addTable(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setNumReplicas(1).build());
  }

  private File buildSegment(String tableName, String segmentName, String artifactName, int value)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension("value", DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    File outputDir = new File(_testDir, artifactName + "-output");
    generatorConfig.setOutDir(outputDir.getAbsolutePath());
    generatorConfig.setSegmentName(segmentName);

    GenericRow row = new GenericRow();
    row.putValue("value", value);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(generatorConfig, new GenericRowRecordReader(List.of(row)));
    driver.build();

    File segmentTar = new File(_testDir, artifactName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(new File(outputDir, segmentName), segmentTar);
    return segmentTar;
  }

  private File createBatchMetadataTar(File segmentTar, String segmentName, String segmentUri)
      throws IOException {
    File metadataDir = new File(_testDir, segmentName + "-batch-metadata");
    FileUtils.forceMkdir(metadataDir);
    TarCompressionUtils.untarOneFile(segmentTar, V1Constants.SEGMENT_CREATION_META,
        new File(metadataDir, segmentName + "." + V1Constants.SEGMENT_CREATION_META));
    TarCompressionUtils.untarOneFile(segmentTar, V1Constants.MetadataKeys.METADATA_FILE_NAME,
        new File(metadataDir, segmentName + "." + V1Constants.MetadataKeys.METADATA_FILE_NAME));
    Files.writeString(new File(metadataDir, SegmentUploadConstants.ALL_SEGMENTS_METADATA_FILENAME).toPath(),
        segmentName + System.lineSeparator() + segmentUri + System.lineSeparator(), StandardCharsets.UTF_8);
    File metadataTar = new File(_testDir,
        SegmentUploadConstants.ALL_SEGMENTS_METADATA_TAR_FILE_PREFIX + segmentName
            + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(metadataDir, metadataTar);
    return metadataTar;
  }

  private static SimpleHttpResponse uploadMultipart(FileUploadDownloadClient client, URI uploadUri, File segment,
      String tableName, List<Header> headers)
      throws Exception {
    return client.uploadSegment(uploadUri, segment.getName(), segment, headers,
        uploadParameters(tableName), HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static SimpleHttpResponse uploadJson(FileUploadDownloadClient client, URI uploadUri, String segmentUri,
      String tableName, List<Header> headers)
      throws Exception {
    URI requestUri = new URIBuilder(uploadUri).addParameters(uploadParameters(tableName)).build();
    return client.sendSegmentUri(requestUri, segmentUri, headers, null,
        HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static List<NameValuePair> uploadParameters(String tableName) {
    NameValuePair tableType =
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, TableType.OFFLINE.name());
    return tableName == null ? List.of(tableType)
        : List.of(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName), tableType);
  }

  private static HttpServer startSegmentServer(Map<String, File> segments, AtomicInteger segmentDownloadCount)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    for (Map.Entry<String, File> entry : segments.entrySet()) {
      server.createContext("/" + entry.getKey(), exchange -> {
        segmentDownloadCount.incrementAndGet();
        long length = entry.getValue().length();
        exchange.sendResponseHeaders(Response.Status.OK.getStatusCode(), length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
          Files.copy(entry.getValue().toPath(), outputStream);
        }
      });
    }
    server.start();
    return server;
  }

  private static String segmentUrl(HttpServer server, String segmentName) {
    return "http://localhost:" + server.getAddress().getPort() + "/" + segmentName;
  }

  private void assertNoSegmentState(String rawTableName, String tableNameWithType, String segmentName) {
    Assert.assertNull(_helixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName));
    Assert.assertFalse(new File(new File(_controllerConfig.getDataDir(), rawTableName), segmentName).exists());
  }

  private static HttpErrorStatusException assertHttpStatus(Response.Status expectedStatus, ThrowingRunnable request)
      throws Exception {
    try {
      request.run();
      Assert.fail("Expected HTTP status " + expectedStatus);
      return null;
    } catch (HttpErrorStatusException e) {
      Assert.assertEquals(e.getStatusCode(), expectedStatus.getStatusCode(), e.getMessage());
      return e;
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_testDir);
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run()
        throws Exception;
  }

  public static class DestinationAccessControlFactory implements AccessControlFactory {
    private static volatile Set<String> _allowedTables = Set.of(TABLE_A);

    static void setAllowedTables(Set<String> allowedTables) {
      _allowedTables = allowedTables;
    }

    @Override
    public AccessControl create() {
      return new AccessControl() {
        @Override
        public boolean protectAnnotatedOnly() {
          return false;
        }

        @Override
        public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders,
            String endpointUrl) {
          return tableName == null || _allowedTables.contains(tableName);
        }

        @Override
        public boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
          return true;
        }

        @Override
        public boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
          return targetType == TargetType.CLUSTER || _allowedTables.contains(targetId);
        }

        @Override
        public boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType) {
          return true;
        }
      };
    }
  }
}
