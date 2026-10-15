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
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.ByteArrayInputStream;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.segment.local.constants.SegmentUploadConstants;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TlsSpec;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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
    assertEquals(nameValuePair.getName(),
        FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION);
    assertEquals(nameValuePair.getValue(), "false");
    pushJobSpec.setPushParallelism(2);
    nameValuePair = FileUploadDownloadClient.makeParallelProtectionParam(pushJobSpec);
    assertEquals(nameValuePair.getName(),
        FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION);
    assertEquals(nameValuePair.getValue(), "true");
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
    return createHttpsServer(handlers, null);
  }

  private static HttpsServer createHttpsServer(Map<String, HttpHandler> handlers, ExecutorService executor)
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
    server.setExecutor(executor);
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
    assertNotNull(resource, "Missing TLS test resource: " + fileName);
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
  public void testGenerateSegmentMetadataFileFromLocalSegment()
      throws Exception {
    // A v3 segment layout with a data file that must not be included
    File segmentDir = new File(_tempDir, TEST_SEGMENT_NAME);
    File v3Dir = new File(segmentDir, "v3");
    FileUtils.forceMkdir(v3Dir);
    FileUtils.writeStringToFile(new File(v3Dir, "metadata.properties"), "segment.name = " + TEST_SEGMENT_NAME,
        StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(v3Dir, "creation.meta"), "crc", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(v3Dir, "columns.psf"), "data", StandardCharsets.UTF_8);

    File metadataTar = SegmentPushUtils.generateSegmentMetadataFile(segmentDir, _tempDir, TEST_SEGMENT_NAME);

    assertEquals(metadataTar.getName(), TEST_SEGMENT_NAME + Constants.METADATA_TAR_GZ_FILE_EXT);
    File untarred = TarCompressionUtils.untar(metadataTar, new File(_tempDir, "untar")).get(0);
    assertEquals(new HashSet<>(Arrays.asList(untarred.list())), Set.of("metadata.properties", "creation.meta"));
    assertEquals(FileUtils.readFileToString(new File(untarred, "metadata.properties"), StandardCharsets.UTF_8),
        "segment.name = " + TEST_SEGMENT_NAME);
  }

  @Test
  public void testSendSegmentUriAndMetadataWithLocalMetadataFiles()
      throws Exception {
    List<Headers> requests = new CopyOnWriteArrayList<>();
    HttpsServer httpsServer = createHttpsServer("/v2/segments", new RecordingHandler(requests));
    try {
      URI controllerUri = new URI("https://localhost:" + httpsServer.getAddress().getPort());
      SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, createTlsSpec());
      spec.getPushJobSpec().setCopyToDeepStoreForMetadataPush(true);
      File metadataTar = createMetadataTar(TEST_SEGMENT_NAME);

      SegmentPushUtils.sendSegmentUriAndMetadata(spec, Map.of(TEST_SEGMENT_URI, metadataTar), new ArrayList<>(),
          new ArrayList<>());

      assertEquals(requests.size(), 1);
      Headers headers = requests.get(0);
      assertEquals(headers.getFirst(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE),
          FileUploadDownloadClient.FileUploadType.METADATA.toString());
      assertEquals(headers.getFirst(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI), TEST_SEGMENT_URI);
      assertEquals(headers.getFirst(FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE), "true");
      // The caller owns the metadata file
      assertTrue(metadataTar.isFile());
    } finally {
      httpsServer.stop(0);
    }
  }

  @Test
  public void testSendSegmentsUriAndMetadataWithLocalMetadataFiles()
      throws Exception {
    List<Headers> requests = new CopyOnWriteArrayList<>();
    HttpsServer httpsServer = createHttpsServer("/segments/batchUpload", new RecordingHandler(requests));
    try {
      URI controllerUri = new URI("https://localhost:" + httpsServer.getAddress().getPort());
      SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, createTlsSpec());
      spec.getPushJobSpec().setPushParallelism(32);
      Map<String, File> segmentUriToMetadataFileMap = new HashMap<>();
      segmentUriToMetadataFileMap.put("file:///tmp/segment1.tar.gz", createMetadataTar("segment1"));
      segmentUriToMetadataFileMap.put("file:///tmp/segment2.tar.gz", createMetadataTar("segment2"));

      SegmentPushUtils.sendSegmentsUriAndMetadata(spec, segmentUriToMetadataFileMap, new ArrayList<>(),
          new ArrayList<>());

      // Existing overloads retain one batch even when distributed callers configure their own parallelism.
      assertEquals(requests.size(), 1);
      assertEquals(requests.get(0).getFirst(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE),
          FileUploadDownloadClient.FileUploadType.METADATA.toString());
      segmentUriToMetadataFileMap.values().forEach(file -> assertTrue(file.isFile()));
    } finally {
      httpsServer.stop(0);
    }
  }

  @DataProvider
  public Object[][] metadataPushTls() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "metadataPushTls", timeOut = 30000)
  public void testParallelMetadataBatchesPreserveSegmentUris(boolean tls)
      throws Exception {
    int parallelism = 32;
    CountDownLatch requestsStarted = new CountDownLatch(parallelism);
    CountDownLatch releaseResponses = new CountDownLatch(1);
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maxActive = new AtomicInteger();
    List<Map<String, String>> receivedBatches = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
    ExecutorService serverExecutor = Executors.newCachedThreadPool();
    ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
    HttpHandler handler = exchange -> {
      maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
      try {
        assertEquals(exchange.getRequestHeaders().getFirst("Authorization"), "Bearer metadata-push-test");
        assertEquals(exchange.getRequestHeaders().getFirst(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE),
            FileUploadDownloadClient.FileUploadType.METADATA.toString());
        assertEquals(exchange.getRequestHeaders().getFirst(
            FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE), "true");
        assertTrue(exchange.getRequestURI().getQuery().contains("enableParallelPushProtection=true"));
        receivedBatches.add(readMetadataBatch(exchange));
        requestsStarted.countDown();
        if (!releaseResponses.await(15, TimeUnit.SECONDS)) {
          throw new IOException("Timed out waiting to release metadata batches");
        }
        writeResponse(exchange, OK_RESPONSE);
      } catch (Exception | AssertionError e) {
        handlerFailure.compareAndSet(null, e);
        requestsStarted.countDown();
      } finally {
        active.decrementAndGet();
        exchange.close();
      }
    };
    HttpServer server = tls ? createHttpsServer(Map.of("/segments/batchUpload", handler), serverExecutor)
        : createMetadataHttpServer(handler, serverExecutor);
    try {
      URI controllerUri = URI.create((tls ? "https" : "http") + "://localhost:" + server.getAddress().getPort());
      SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, tls ? createTlsSpec() : null);
      spec.getPushJobSpec().setPushParallelism(parallelism);
      spec.getPushJobSpec().setCopyToDeepStoreForMetadataPush(true);
      Map<String, File> metadataFiles = createMetadataFiles(65);
      List<NameValuePair> parameters = FileUploadDownloadClient.makeTableParam(TEST_TABLE_NAME);
      parameters.add(FileUploadDownloadClient.makeParallelProtectionParam(spec.getPushJobSpec()));
      Future<?> push = callerExecutor.submit(() -> {
        SegmentPushUtils.sendSegmentsUriAndMetadata(spec, metadataFiles,
            List.of(new BasicHeader("Authorization", "Bearer metadata-push-test")), parameters, parallelism);
        return null;
      });
      assertTrue(requestsStarted.await(15, TimeUnit.SECONDS), "All batch workers must reach one controller together");
      assertNull(handlerFailure.get());
      assertEquals(active.get(), parallelism);
      assertEquals(receivedBatches.size(), parallelism);
      assertFalse(push.isDone());
      releaseResponses.countDown();
      push.get(10, TimeUnit.SECONDS);
      assertNull(handlerFailure.get());
      assertEquals(maxActive.get(), parallelism);
      assertEquals(receivedBatches.size(), parallelism, "Partition once instead of issuing another request wave");
      Map<String, String> receivedSegments = new HashMap<>();
      for (Map<String, String> batch : receivedBatches) {
        assertTrue(batch.size() >= 2 && batch.size() <= 3, "Batches must be balanced and nonempty");
        batch.forEach((name, uri) -> assertNull(receivedSegments.put(name, uri), "Repeated segment " + name));
      }
      assertEquals(receivedSegments, metadataSegmentUris(metadataFiles));
      metadataFiles.values().forEach(file -> assertTrue(file.isFile(), "The caller owns local metadata files"));
    } finally {
      releaseResponses.countDown();
      callerExecutor.shutdownNow();
      callerExecutor.close();
      server.stop(0);
      serverExecutor.shutdownNow();
      serverExecutor.close();
    }
  }

  @Test(timeOut = 10000)
  public void testParallelMetadataBatchesRetryWithoutChangingSegmentUris()
      throws Exception {
    Map<Map<String, String>, AtomicInteger> batchAttempts = new ConcurrentHashMap<>();
    ExecutorService serverExecutor = Executors.newCachedThreadPool();
    HttpServer server = createMetadataHttpServer(exchange -> {
      Map<String, String> batch = readMetadataBatch(exchange);
      int attempt = batchAttempts.computeIfAbsent(batch, ignored -> new AtomicInteger()).incrementAndGet();
      if (attempt < 3) {
        // HttpClient retries 503 internally; use 500 to exercise the push job's configured attempts.
        exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
      } else {
        writeResponse(exchange, OK_RESPONSE);
      }
      exchange.close();
    }, serverExecutor);
    try {
      URI controllerUri = URI.create("http://localhost:" + server.getAddress().getPort());
      SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, null);
      spec.getPushJobSpec().setPushAttempts(3);
      spec.getPushJobSpec().setPushRetryIntervalMillis(1);
      Map<String, File> metadataFiles = createMetadataFiles(3);
      SegmentPushUtils.sendSegmentsUriAndMetadata(spec, metadataFiles, List.of(), List.of(), 2);
      assertEquals(batchAttempts.size(), 2);
      Map<String, String> receivedSegments = new HashMap<>();
      batchAttempts.forEach((batch, attempts) -> {
        assertEquals(attempts.get(), 3, "Each batch must retain its manifest through both retries");
        batch.forEach((name, uri) -> assertNull(receivedSegments.put(name, uri), "Repeated segment " + name));
      });
      assertEquals(receivedSegments, metadataSegmentUris(metadataFiles));
      metadataFiles.values().forEach(file -> assertTrue(file.isFile(), "Retries must preserve caller-owned input"));
    } finally {
      server.stop(0);
      serverExecutor.shutdownNow();
      serverExecutor.close();
    }
  }

  @DataProvider
  public Object[][] serialMetadataPushCases() {
    return new Object[][]{{0, 3}, {1, 3}, {8, 1}, {8, 0}};
  }

  @Test(dataProvider = "serialMetadataPushCases", timeOut = 10000)
  public void testMetadataPushSerialFallback(int parallelism, int segmentCount)
      throws Exception {
    List<Map<String, String>> receivedBatches = new CopyOnWriteArrayList<>();
    ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    HttpServer server = createMetadataHttpServer(exchange -> {
      receivedBatches.add(readMetadataBatch(exchange));
      writeResponse(exchange, OK_RESPONSE);
      exchange.close();
    }, serverExecutor);
    try {
      URI controllerUri = URI.create("http://localhost:" + server.getAddress().getPort());
      SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, null);
      Map<String, File> metadataFiles = createMetadataFiles(segmentCount);
      SegmentPushUtils.sendSegmentsUriAndMetadata(spec, metadataFiles, List.of(), List.of(), parallelism);
      assertEquals(receivedBatches, segmentCount == 0 ? List.of() : List.of(metadataSegmentUris(metadataFiles)));
    } finally {
      server.stop(0);
      serverExecutor.shutdownNow();
      serverExecutor.close();
    }
  }

  @DataProvider
  public Object[][] metadataPushAbortCases() {
    return new Object[][]{{false, false}, {true, false}, {false, true}};
  }

  @Test(dataProvider = "metadataPushAbortCases", timeOut = 15000)
  public void testMetadataPushDrainsActiveRequestsBeforeCleanup(boolean interruptCaller, boolean retryBlockedRequest)
      throws Exception {
    CountDownLatch requestsStarted = new CountDownLatch(2);
    CountDownLatch failureResponded = new CountDownLatch(1);
    CountDownLatch releaseResponses = new CountDownLatch(1);
    CountDownLatch callerExited = new CountDownLatch(1);
    AtomicInteger requests = new AtomicInteger();
    AtomicInteger completedResponses = new AtomicInteger();
    AtomicReference<Exception> failure = new AtomicReference<>();
    AtomicBoolean callerInterrupted = new AtomicBoolean();
    List<File> stagedMetadata = new CopyOnWriteArrayList<>();
    ExecutorService serverExecutor = Executors.newCachedThreadPool();
    HttpServer server = createMetadataHttpServer(exchange -> {
      exchange.getRequestBody().readAllBytes();
      int request = requests.incrementAndGet();
      requestsStarted.countDown();
      try {
        if (!requestsStarted.await(5, TimeUnit.SECONDS)) {
          throw new IOException("The other metadata batch did not start");
        }
        if (!interruptCaller && request == 2) {
          exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
          failureResponded.countDown();
        } else {
          if (!releaseResponses.await(10, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting to release active metadata request");
          }
          if (retryBlockedRequest) {
            // The job retry policy sees this 500 after the other batch has already failed permanently.
            exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
          } else {
            writeResponse(exchange, OK_RESPONSE);
          }
          completedResponses.incrementAndGet();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } finally {
        exchange.close();
      }
    }, serverExecutor);
    URI controllerUri = URI.create("http://localhost:" + server.getAddress().getPort());
    SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, null);
    spec.setPinotClusterSpecs(new PinotClusterSpec[]{spec.getPinotClusterSpecs()[0], spec.getPinotClusterSpecs()[0]});
    spec.getPushJobSpec().setBatchSegmentUpload(true);
    spec.getPushJobSpec().setPreferMetadataTarGz(true);
    spec.getPushJobSpec().setSegmentMetadataGenerationParallelism(2);
    spec.getPushJobSpec().setPushAttempts(2);
    spec.getPushJobSpec().setPushRetryIntervalMillis(1);
    Map<String, File> metadataFiles = createMetadataFiles(2);
    Map<String, String> segmentPaths = new HashMap<>();
    metadataFiles.forEach((uri, file) -> segmentPaths.put(uri,
        new File(file.getParentFile(), file.getName().replace(Constants.METADATA_TAR_GZ_FILE_EXT, ".tar.gz"))
            .toURI().toString()));
    LocalPinotFS fileSystem = new LocalPinotFS() {
      @Override
      public void copyToLocalFile(URI srcUri, File dstFile)
          throws Exception {
        super.copyToLocalFile(srcUri, dstFile);
        stagedMetadata.add(dstFile);
      }
    };
    Thread caller = new Thread(() -> {
      try {
        SegmentPushUtils.sendSegmentUriAndMetadata(spec, fileSystem, segmentPaths, 2);
      } catch (Exception e) {
        failure.set(e);
      } finally {
        callerInterrupted.set(Thread.currentThread().isInterrupted());
        callerExited.countDown();
      }
    });
    try {
      caller.start();
      assertTrue(requestsStarted.await(5, TimeUnit.SECONDS));
      if (interruptCaller) {
        caller.interrupt();
      } else {
        assertTrue(failureResponded.await(5, TimeUnit.SECONDS));
      }
      assertFalse(callerExited.await(200, TimeUnit.MILLISECONDS), "Cleanup must wait for every active HTTP request");
      assertEquals(stagedMetadata.size(), 2);
      stagedMetadata.forEach(file -> assertTrue(file.isFile(), "Active batches still own their staged input"));
      releaseResponses.countDown();
      caller.join(5000);
      assertFalse(caller.isAlive());
      assertNotNull(failure.get());
      assertEquals(callerInterrupted.get(), interruptCaller);
      assertEquals(completedResponses.get(), interruptCaller ? 2 : 1);
      assertEquals(requests.get(), 2, "Failure or interruption must suppress retries and subsequent cluster requests");
      stagedMetadata.forEach(file -> assertFalse(file.exists(), "Delete staged metadata after the workers finish"));
      metadataFiles.values().forEach(file -> assertTrue(file.isFile(), "Keep the caller's original metadata"));
    } finally {
      releaseResponses.countDown();
      caller.interrupt();
      caller.join(5000);
      server.stop(0);
      serverExecutor.shutdownNow();
      serverExecutor.close();
    }
  }

  @Test(timeOut = 10000)
  public void testParallelMetadataPushRejectsInvalidInputBeforeHttp()
      throws Exception {
    AtomicInteger requests = new AtomicInteger();
    ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    HttpServer server = createMetadataHttpServer(exchange -> {
      requests.incrementAndGet();
      exchange.getRequestBody().readAllBytes();
      writeResponse(exchange, OK_RESPONSE);
      exchange.close();
    }, serverExecutor);
    try {
      File metadata = createMetadataTar(TEST_SEGMENT_NAME);
      File duplicate = new File(_tempDir, "duplicate/" + metadata.getName());
      FileUtils.copyFile(metadata, duplicate);
      URI controllerUri = URI.create("http://localhost:" + server.getAddress().getPort());
      SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(controllerUri, null);
      Map<String, File> metadataFiles = Map.of("file:///first/segment.tar.gz", metadata,
          "file:///second/segment.tar.gz", duplicate);
      assertThrows(IllegalArgumentException.class,
          () -> SegmentPushUtils.sendSegmentsUriAndMetadata(spec, metadataFiles, List.of(), List.of(), 2));

      PinotFS fileSystem = mock(PinotFS.class);
      Map<String, String> invalidUris = Map.of("file:///invalid uri/segment.tar.gz", "segment.tar.gz");
      assertThrows(IllegalArgumentException.class,
          () -> SegmentPushUtils.sendSegmentsUriAndMetadata(spec, fileSystem, invalidUris, List.of(), List.of()));
      assertThrows(IllegalArgumentException.class,
          () -> SegmentPushUtils.sendSegmentsUriAndMetadata(spec,
              Map.of("file:///invalid uri/segment.tar.gz", metadata), List.of(), List.of()));

      spec.getPinotClusterSpecs()[0].setControllerURI("file:///invalid-controller");
      assertThrows(IllegalArgumentException.class,
          () -> SegmentPushUtils.sendSegmentsUriAndMetadata(spec, fileSystem,
              Map.of(TEST_SEGMENT_URI, "segment.tar.gz"), List.of(), List.of()));
      assertThrows(IllegalArgumentException.class,
          () -> SegmentPushUtils.sendSegmentsUriAndMetadata(spec, Map.of(TEST_SEGMENT_URI, metadata), List.of(),
              List.of()));
      verifyNoInteractions(fileSystem);
      assertEquals(requests.get(), 0);
      assertTrue(metadata.isFile());
      assertTrue(duplicate.isFile());
    } finally {
      server.stop(0);
      serverExecutor.shutdownNow();
      serverExecutor.close();
    }
  }

  private static HttpServer createMetadataHttpServer(HttpHandler handler, ExecutorService executor)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/segments/batchUpload", handler);
    server.setExecutor(executor);
    server.start();
    return server;
  }

  private Map<String, File> createMetadataFiles(int count)
      throws IOException {
    Map<String, File> metadataFiles = new HashMap<>();
    for (int i = 0; i < count; i++) {
      String segmentName = "segment" + i;
      metadataFiles.put("file:///download/" + segmentName + ".tar.gz", createMetadataTar(segmentName));
    }
    return metadataFiles;
  }

  private static Map<String, String> metadataSegmentUris(Map<String, File> metadataFiles) {
    Map<String, String> segmentUris = new HashMap<>();
    metadataFiles.forEach((uri, file) -> segmentUris.put(
        file.getName().substring(0, file.getName().length() - Constants.METADATA_TAR_GZ_FILE_EXT.length()), uri));
    return segmentUris;
  }

  private Map<String, String> readMetadataBatch(HttpExchange exchange)
      throws IOException {
    byte[] requestBody = exchange.getRequestBody().readAllBytes();
    String multipart = new String(requestBody, StandardCharsets.ISO_8859_1);
    int partHeadersEnd = multipart.indexOf("\r\n\r\n");
    int partEnd = multipart.lastIndexOf("\r\n--");
    assertTrue(partHeadersEnd >= 0 && partEnd > partHeadersEnd + 4, "Expected one multipart metadata TAR");
    File unpacked = TarCompressionUtils.untar(
        new ByteArrayInputStream(requestBody, partHeadersEnd + 4, partEnd - partHeadersEnd - 4),
        new File(_tempDir, "received-" + UUID.randomUUID())).get(0);
    List<String> lines = Files.readAllLines(new File(unpacked,
        SegmentUploadConstants.ALL_SEGMENTS_METADATA_FILENAME).toPath(), StandardCharsets.UTF_8);
    assertEquals(lines.size() % 2, 0);
    Map<String, String> segmentUris = new HashMap<>();
    for (int i = 0; i < lines.size(); i += 2) {
      String name = lines.get(i);
      assertNull(segmentUris.put(name, lines.get(i + 1)), "Repeated name within a batch");
      assertTrue(new File(unpacked, name + ".metadata.properties").isFile());
      assertTrue(new File(unpacked, name + ".creation.meta").isFile());
    }
    assertEquals(unpacked.list().length, 2 * segmentUris.size() + 1);
    return segmentUris;
  }

  @Test
  public void testSendSegmentUriAndMetadataRejectsUnnamedMetadataFile() {
    SegmentGenerationJobSpec spec = createSegmentGenerationJobSpec(URI.create("https://localhost:1"), null);
    File notAMetadataTar = new File(_tempDir, "segment.tar.gz");
    assertThrows(IllegalArgumentException.class, () -> SegmentPushUtils.sendSegmentUriAndMetadata(spec,
        Map.of(TEST_SEGMENT_URI, notAMetadataTar), new ArrayList<>(), new ArrayList<>()));
  }

  private File createMetadataTar(String segmentName)
      throws IOException {
    File segmentDir = new File(_tempDir, segmentName);
    FileUtils.forceMkdir(segmentDir);
    FileUtils.touch(new File(segmentDir, "metadata.properties"));
    FileUtils.touch(new File(segmentDir, "creation.meta"));
    return SegmentPushUtils.generateSegmentMetadataFile(segmentDir, _tempDir, segmentName);
  }

  /// Records request headers and answers OK, for both single and batch metadata pushes.
  private static class RecordingHandler implements HttpHandler {
    private final List<Headers> _requests;

    private RecordingHandler(List<Headers> requests) {
      _requests = requests;
    }

    @Override
    public void handle(HttpExchange httpExchange)
        throws IOException {
      _requests.add(httpExchange.getRequestHeaders());
      httpExchange.getRequestBody().readAllBytes();
      writeResponse(httpExchange, OK_RESPONSE);
    }
  }

  @Test
  public void testGetOrCreateFileUploadDownloadClientUsesSharedDefaultClient()
      throws Exception {
    SegmentGenerationJobSpec defaultJobSpec = new SegmentGenerationJobSpec();
    FileUploadDownloadClient defaultClient = SegmentPushUtils.getOrCreateFileUploadDownloadClient(defaultJobSpec);
    assertSame(SegmentPushUtils.getOrCreateFileUploadDownloadClient(defaultJobSpec), defaultClient);
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
    PinotFS mockFs = mock(PinotFS.class);
    Map<String, String> segmentUriToTarPathMap = Map.of(
        "file:///download/segment1.tar.gz", "segment1.tar.gz",
        "file:///download/segment2.tar.gz", "segment2.tar.gz"
    );
    ConcurrentHashMap<String, File> segmentMetadataFileMap = new ConcurrentHashMap<>();
    Thread producer = Thread.currentThread();
    CountDownLatch namesPublished = new CountDownLatch(2);
    ConcurrentLinkedQueue<String> segmentURIs = new ConcurrentLinkedQueue<>() {
      @Override
      public boolean add(String value) {
        boolean added = super.add(value);
        if (Thread.currentThread() != producer && !value.contains("://")) {
          namesPublished.countDown();
          try {
            assertTrue(namesPublished.await(10, TimeUnit.SECONDS), "Both names must publish before either URI");
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        }
        return added;
      }
    };

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
    List<String> entries = new ArrayList<>(segmentURIs);
    for (int i = 0; i < entries.size(); i += 2) {
      assertEquals(segmentUriToTarPathMap.get(entries.get(i + 1)), entries.get(i) + ".tar.gz",
          "Every segment name must remain adjacent to its own download URI");
    }
  }

  @Test
  public void testGenerateSegmentMetadataFilesFailure()
      throws Exception {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFS mockFs = mock(PinotFS.class);
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
      RuntimeException failure = expectThrows(RuntimeException.class,
          () -> SegmentPushUtils.generateSegmentMetadataFiles(jobSpec, mockFs, segmentUriToTarPathMap,
              segmentMetadataFileMap, segmentURIs, executor));
      assertEquals(failure.getMessage(), "1 out of 2 segment metadata generation failed");
    } finally {
      executor.shutdown();
    }

    assertEquals(segmentMetadataFileMap.size(), 1);
    assertTrue(segmentURIs.isEmpty(), "Do not publish a partial download manifest after metadata generation fails");
  }
}
