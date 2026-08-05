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
package org.apache.pinot.segment.local.utils;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TlsSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class SegmentPushUtilsTest {
  private static final String TLS_RESOURCE_FOLDER = "tls/";
  private static final String TLS_KEYSTORE_FILE = "keystore.p12";
  private static final String TLS_TRUSTSTORE_FILE = "truststore.p12";
  private static final String TLS_STORE_PASSWORD = "changeit";
  private static final String TLS_STORE_TYPE = "PKCS12";
  private static final String TEST_TABLE_NAME = "testTable";
  private static final String TEST_SEGMENT_NAME = "testSegment";
  private static final String TEST_SEGMENT_URI = "file:///tmp/testSegment.tar.gz";
  private static final String TEST_LINEAGE_ENTRY_ID = "lineageEntry";
  private static final String OK_RESPONSE = "OK";

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "test-" + UUID.randomUUID());
    FileUtils.forceMkdir(_tempDir);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testSegmentParallelProtectionUploadParam() {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    NameValuePair nameValuePair = FileUploadDownloadClient.makeParallelProtectionParam(pushJobSpec);
    Assert.assertEquals(nameValuePair.getName(),
        FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION);
    Assert.assertEquals(nameValuePair.getValue(), "false");
    pushJobSpec.setPushParallelism(2);
    nameValuePair = FileUploadDownloadClient.makeParallelProtectionParam(pushJobSpec);
    Assert.assertEquals(nameValuePair.getName(),
        FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION);
    Assert.assertEquals(nameValuePair.getValue(), "true");
  }

  @Test
  public void testSendSegmentUrisHonorsTlsSpec()
      throws Exception {
    AtomicBoolean receivedTlsRequest = new AtomicBoolean();
    HttpsServer httpsServer =
        createHttpsServer("/v2/segments", new TestSegmentUploadHandler(receivedTlsRequest));
    try {
      URI controllerUri = new URI("https://localhost:" + httpsServer.getAddress().getPort());

      SegmentGenerationJobSpec defaultJobSpec = createSegmentGenerationJobSpec(controllerUri, null);
      assertThrows(Exception.class, () -> SegmentPushUtils.sendSegmentUris(defaultJobSpec, List.of(TEST_SEGMENT_URI)));

      SegmentGenerationJobSpec tlsJobSpec = createSegmentGenerationJobSpec(controllerUri, createTlsSpec());
      SegmentPushUtils.sendSegmentUris(tlsJobSpec, List.of(TEST_SEGMENT_URI));
      assertTrue(receivedTlsRequest.get());
    } finally {
      httpsServer.stop(0);
    }
  }

  @Test
  public void testConsistentDataPushGetSegmentsToReplaceHonorsTlsSpec()
      throws Exception {
    AtomicBoolean receivedTlsRequest = new AtomicBoolean();
    HttpsServer httpsServer =
        createHttpsServer("/segments/" + TEST_TABLE_NAME, new TestSegmentListHandler(receivedTlsRequest));
    try {
      URI controllerUri = new URI("https://localhost:" + httpsServer.getAddress().getPort());

      SegmentGenerationJobSpec tlsJobSpec = createSegmentGenerationJobSpec(controllerUri, createTlsSpec());
      Map<URI, List<String>> segmentsToReplace =
          ConsistentDataPushUtils.getSegmentsToReplace(tlsJobSpec, TEST_TABLE_NAME);

      assertEquals(segmentsToReplace.get(controllerUri), List.of(TEST_SEGMENT_NAME));
      assertTrue(receivedTlsRequest.get());
    } finally {
      httpsServer.stop(0);
    }
  }

  @Test(timeOut = 5000)
  public void testSendSegmentUrisHonorsTlsSpecReadTimeout()
      throws Exception {
    AtomicBoolean receivedTlsRequest = new AtomicBoolean();
    CountDownLatch releaseResponse = new CountDownLatch(1);
    HttpsServer httpsServer =
        createHttpsServer("/v2/segments", new TestSegmentUploadHandler(receivedTlsRequest, releaseResponse));
    try {
      URI controllerUri = new URI("https://localhost:" + httpsServer.getAddress().getPort());
      TlsSpec tlsSpec = createTlsSpec();
      tlsSpec.setReadTimeout(100);
      SegmentGenerationJobSpec tlsJobSpec = createSegmentGenerationJobSpec(controllerUri, tlsSpec);

      assertThrows(Exception.class, () -> SegmentPushUtils.sendSegmentUris(tlsJobSpec, List.of(TEST_SEGMENT_URI)));
      assertTrue(receivedTlsRequest.get());
    } finally {
      releaseResponse.countDown();
      httpsServer.stop(0);
    }
  }

  @Test
  public void testTlsSpecConnectTimeoutConfigIsApplied() {
    TlsSpec tlsSpec = createTlsSpec();
    tlsSpec.setConnectTimeout(1234);

    HttpClientConfig httpClientConfig = SegmentPushUtils.getHttpClientConfig(tlsSpec);

    assertEquals(httpClientConfig.getConnectionTimeoutMs(), 1234);
  }

  @Test
  public void testConsistentDataPushStartEndRevertHonorsTlsSpec()
      throws Exception {
    AtomicBoolean receivedStartRequest = new AtomicBoolean();
    AtomicBoolean receivedEndRequest = new AtomicBoolean();
    AtomicBoolean receivedRevertRequest = new AtomicBoolean();
    HttpsServer httpsServer = createHttpsServer(Map.of(
        "/segments/" + TEST_TABLE_NAME + "/startReplaceSegments",
        new TestConsistentDataPushHandler(receivedStartRequest,
            "{\"segmentLineageEntryId\":\"" + TEST_LINEAGE_ENTRY_ID + "\"}", "type=OFFLINE", "forceCleanup=true"),
        "/segments/" + TEST_TABLE_NAME + "/endReplaceSegments",
        new TestConsistentDataPushHandler(receivedEndRequest, OK_RESPONSE,
            "segmentLineageEntryId=" + TEST_LINEAGE_ENTRY_ID, "cleanup=false"),
        "/segments/" + TEST_TABLE_NAME + "/revertReplaceSegments",
        new TestConsistentDataPushHandler(receivedRevertRequest, OK_RESPONSE,
            "segmentLineageEntryId=" + TEST_LINEAGE_ENTRY_ID, "forceRevert=true")));
    try {
      URI controllerUri = new URI("https://localhost:" + httpsServer.getAddress().getPort());
      SegmentGenerationJobSpec tlsJobSpec = createSegmentGenerationJobSpec(controllerUri, createTlsSpec());

      Map<URI, String> lineageEntryIds = ConsistentDataPushUtils.startReplaceSegments(tlsJobSpec,
          Map.of(controllerUri, List.of("oldSegment")), List.of(TEST_SEGMENT_NAME));
      assertEquals(lineageEntryIds.get(controllerUri), TEST_LINEAGE_ENTRY_ID);

      ConsistentDataPushUtils.endReplaceSegments(tlsJobSpec, lineageEntryIds);
      ConsistentDataPushUtils.handleUploadException(tlsJobSpec, lineageEntryIds, new RuntimeException("test"));

      assertTrue(receivedStartRequest.get());
      assertTrue(receivedEndRequest.get());
      assertTrue(receivedRevertRequest.get());
    } finally {
      httpsServer.stop(0);
    }
  }

  private static HttpsServer createHttpsServer(String path, HttpHandler handler)
      throws Exception {
    return createHttpsServer(Map.of(path, handler));
  }

  private static HttpsServer createHttpsServer(Map<String, HttpHandler> handlers)
      throws Exception {
    SSLContext sslContext = createTestSslContext();
    HttpsServer server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
    server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
      @Override
      public void configure(HttpsParameters params) {
        SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
        sslParameters.setNeedClientAuth(true);
        params.setSSLParameters(sslParameters);
      }
    });
    handlers.forEach(server::createContext);
    server.setExecutor(null);
    server.start();
    return server;
  }

  private static SegmentGenerationJobSpec createSegmentGenerationJobSpec(URI controllerUri, TlsSpec tlsSpec) {
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(TEST_TABLE_NAME);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(controllerUri.toString());

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setTableSpec(tableSpec);
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{pinotClusterSpec});
    jobSpec.setPushJobSpec(new PushJobSpec());
    jobSpec.setTlsSpec(tlsSpec);
    return jobSpec;
  }

  private static SSLContext createTestSslContext()
      throws Exception {
    KeyManagerFactory keyManagerFactory =
        TlsUtils.createKeyManagerFactory(getTlsResourcePath(TLS_KEYSTORE_FILE), TLS_STORE_PASSWORD, TLS_STORE_TYPE);
    TrustManagerFactory trustManagerFactory =
        TlsUtils.createTrustManagerFactory(getTlsResourcePath(TLS_TRUSTSTORE_FILE), TLS_STORE_PASSWORD, TLS_STORE_TYPE);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
    return sslContext;
  }

  private static TlsSpec createTlsSpec() {
    TlsSpec tlsSpec = new TlsSpec();
    tlsSpec.setKeyStoreType(TLS_STORE_TYPE);
    tlsSpec.setKeyStorePath(getTlsResourcePath(TLS_KEYSTORE_FILE));
    tlsSpec.setKeyStorePassword(TLS_STORE_PASSWORD);
    tlsSpec.setTrustStoreType(TLS_STORE_TYPE);
    tlsSpec.setTrustStorePath(getTlsResourcePath(TLS_TRUSTSTORE_FILE));
    tlsSpec.setTrustStorePassword(TLS_STORE_PASSWORD);
    return tlsSpec;
  }

  private static String getTlsResourcePath(String fileName) {
    URL resource = SegmentPushUtilsTest.class.getClassLoader().getResource(TLS_RESOURCE_FOLDER + fileName);
    Assert.assertNotNull(resource, "Missing TLS test resource: " + fileName);
    return resource.toString();
  }

  private static class TestSegmentUploadHandler implements HttpHandler {
    private final AtomicBoolean _receivedTlsRequest;
    private final CountDownLatch _releaseResponse;

    private TestSegmentUploadHandler(AtomicBoolean receivedTlsRequest) {
      this(receivedTlsRequest, null);
    }

    private TestSegmentUploadHandler(AtomicBoolean receivedTlsRequest, CountDownLatch releaseResponse) {
      _receivedTlsRequest = receivedTlsRequest;
      _releaseResponse = releaseResponse;
    }

    @Override
    public void handle(HttpExchange httpExchange)
        throws IOException {
      Headers requestHeaders = httpExchange.getRequestHeaders();
      assertEquals(requestHeaders.getFirst(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE),
          FileUploadDownloadClient.FileUploadType.URI.toString());
      assertEquals(requestHeaders.getFirst(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI), TEST_SEGMENT_URI);
      _receivedTlsRequest.set(true);
      if (_releaseResponse != null) {
        try {
          _releaseResponse.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }

      writeResponse(httpExchange, OK_RESPONSE);
    }
  }

  private static class TestConsistentDataPushHandler implements HttpHandler {
    private final AtomicBoolean _receivedTlsRequest;
    private final String _responseBody;
    private final List<String> _expectedQueryFragments;

    private TestConsistentDataPushHandler(AtomicBoolean receivedTlsRequest, String responseBody,
        String... expectedQueryFragments) {
      _receivedTlsRequest = receivedTlsRequest;
      _responseBody = responseBody;
      _expectedQueryFragments = Arrays.asList(expectedQueryFragments);
    }

    @Override
    public void handle(HttpExchange httpExchange)
        throws IOException {
      assertEquals(httpExchange.getRequestMethod(), "POST");
      String query = httpExchange.getRequestURI().getQuery();
      for (String expectedQueryFragment : _expectedQueryFragments) {
        assertTrue(query.contains(expectedQueryFragment), query);
      }
      _receivedTlsRequest.set(true);

      writeResponse(httpExchange, _responseBody);
    }
  }

  private static class TestSegmentListHandler implements HttpHandler {
    private final AtomicBoolean _receivedTlsRequest;

    private TestSegmentListHandler(AtomicBoolean receivedTlsRequest) {
      _receivedTlsRequest = receivedTlsRequest;
    }

    @Override
    public void handle(HttpExchange httpExchange)
        throws IOException {
      _receivedTlsRequest.set(true);
      writeResponse(httpExchange, "[{\"OFFLINE\":[\"" + TEST_SEGMENT_NAME + "\"]}]");
    }
  }

  private static void writeResponse(HttpExchange httpExchange, String responseBody)
      throws IOException {
    byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
    httpExchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
    try (OutputStream os = httpExchange.getResponseBody()) {
      os.write(response);
    }
  }

  @Test
  public void testGetOrCreateFileUploadDownloadClientUsesSharedDefaultClient()
      throws Exception {
    SegmentGenerationJobSpec defaultJobSpec = new SegmentGenerationJobSpec();
    FileUploadDownloadClient defaultClient = SegmentPushUtils.getOrCreateFileUploadDownloadClient(defaultJobSpec);
    Assert.assertSame(SegmentPushUtils.getOrCreateFileUploadDownloadClient(defaultJobSpec), defaultClient);
  }

  @Test
  public void testGetSegmentUriToTarPathMap()
      throws IOException {
    URI outputDirURI = Files.createTempDirectory("test").toUri();

    String[] segmentFiles = new String[]{
        outputDirURI.resolve("segment.tar.gz").toString(),
        outputDirURI.resolve("stats_202201.tar.gz").toString(),
        outputDirURI.resolve("/2022/segment.tar.gz").toString(),
        outputDirURI.resolve("/2022/stats_202201.tar.gz").toString()
    };

    PushJobSpec pushSpec = new PushJobSpec();
    Map<String, String> result = SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, pushSpec, segmentFiles);
    assertEquals(result.size(), 4);
    for (String segmentFile : segmentFiles) {
      assertTrue(result.containsKey(segmentFile));
      assertEquals(result.get(segmentFile), segmentFile);
    }

    pushSpec.setPushFileNamePattern("glob:**/2022/*.tar.gz");
    result = SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, pushSpec, segmentFiles);
    assertEquals(result.size(), 2);
    assertEquals(result.get(segmentFiles[2]), segmentFiles[2]);
    assertEquals(result.get(segmentFiles[3]), segmentFiles[3]);

    pushSpec.setPushFileNamePattern("glob:**/stats_*.tar.gz");
    result = SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, pushSpec, segmentFiles);
    assertEquals(result.size(), 2);
    assertEquals(result.get(segmentFiles[1]), segmentFiles[1]);
    assertEquals(result.get(segmentFiles[3]), segmentFiles[3]);
  }

  @Test
  public void testGenerateSegmentMetadataURI()
      throws URISyntaxException {
    assertEquals(
        SegmentPushUtils.generateSegmentMetadataURI("/a/b/c/my-segment.tar.gz", "my-segment"),
        URI.create("/a/b/c/my-segment.metadata.tar.gz"));

    assertEquals(
        SegmentPushUtils.generateSegmentMetadataURI("s3://a/b/c/my-segment.tar.gz", "my-segment"),
        URI.create("s3://a/b/c/my-segment.metadata.tar.gz"));

    assertEquals(
        SegmentPushUtils.generateSegmentMetadataURI("hdfs://a/b/c/my-segment.tar.gz", "my-segment"),
        URI.create("hdfs://a/b/c/my-segment.metadata.tar.gz"));
  }

  @Test
  public void testCreateSegmentsMetadataTarFile()
      throws IOException {
    // setup
    List<String> segmentURIs = Arrays.asList("http://example.com/segment1", "http://example.com/segment2");

    Map<String, File> segmentMetadataFileMap = new HashMap<>();

    File segment1 = new File(_tempDir, "segment1");
    FileUtils.forceMkdir(segment1);
    File creationMetaSeg1 = new File(segment1, "creation.meta");
    FileUtils.touch(creationMetaSeg1);
    File metadataPropertiesSeg1 = new File(segment1, "metadata.properties");
    FileUtils.touch(metadataPropertiesSeg1);
    File segment1Tar = new File(segment1, "segment1.tar.gz");
    TarCompressionUtils.createCompressedTarFile(segment1, segment1Tar);

    File segment2 = new File(_tempDir, "segment2");
    FileUtils.forceMkdir(segment2);
    File creationMetaSeg2 = new File(segment2, "creation.meta");
    FileUtils.touch(creationMetaSeg2);
    File metadataPropertiesSeg2 = new File(segment2, "metadata.properties");
    FileUtils.touch(metadataPropertiesSeg2);
    File segment2Tar = new File(segment2, "segment2.tar.gz");
    TarCompressionUtils.createCompressedTarFile(segment2, segment2Tar);

    segmentMetadataFileMap.put("segment1", segment1Tar);
    segmentMetadataFileMap.put("segment2", segment2Tar);

    // test
    File result = SegmentPushUtils.createSegmentsMetadataTarFile(segmentURIs, segmentMetadataFileMap);

    // verify
    assertTrue(result.exists(), "The resulting tar.gz file should exist");
    assertTrue(result.getName().endsWith(".tar.gz"), "The resulting file should have a .tar.gz extension");
  }

  @Test
  public void testGenerateSegmentMetadataFiles()
      throws Exception {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setSegmentMetadataGenerationParallelism(2);
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFS mockFs = Mockito.mock(PinotFS.class);
    Map<String, String> segmentUriToTarPathMap = Map.of(
        "segment1", "segment1.tar.gz",
        "segment2", "segment2.tar.gz"
    );
    ConcurrentHashMap<String, File> segmentMetadataFileMap = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> segmentURIs = new ConcurrentLinkedQueue<>();

    when(mockFs.exists(any(URI.class))).thenReturn(true);
    mockFs.copyToLocalFile(any(URI.class), any(File.class));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      SegmentPushUtils.generateSegmentMetadataFiles(jobSpec, mockFs, segmentUriToTarPathMap, segmentMetadataFileMap,
          segmentURIs, executor);
    } finally {
      executor.shutdown();
    }

    assertEquals(segmentMetadataFileMap.size(), 2);
    assertEquals(segmentURIs.size(), 4);
  }

  @Test
  public void testGenerateSegmentMetadataFilesFailure()
      throws Exception {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFS mockFs = Mockito.mock(PinotFS.class);
    Map<String, String> segmentUriToTarPathMap = Map.of(
        "segment1", "segment1.tar.gz",
        "segment2", "segment2.tar.gz"
    );
    ConcurrentHashMap<String, File> segmentMetadataFileMap = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> segmentURIs = new ConcurrentLinkedQueue<>();

    when(mockFs.exists(any(URI.class))).thenReturn(true);
    doNothing().doThrow(new RuntimeException("test exception"))
        .when(mockFs)
        .copyToLocalFile(any(URI.class), any(File.class));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      SegmentPushUtils.generateSegmentMetadataFiles(jobSpec, mockFs, segmentUriToTarPathMap, segmentMetadataFileMap,
          segmentURIs, executor);
    } catch (Exception e) {
      assertEquals(e.getMessage(), "1 out of 2 segment metadata generation failed");
    } finally {
      executor.shutdown();
    }

    assertEquals(segmentMetadataFileMap.size(), 1);
    assertEquals(segmentURIs.size(), 2);
  }
}
