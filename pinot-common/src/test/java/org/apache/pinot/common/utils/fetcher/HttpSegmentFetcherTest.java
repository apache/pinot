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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.fetcher.HttpSegmentFetcher.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class HttpSegmentFetcherTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final File SEGMENT_FILE = new File(FileUtils.getTempDirectory(), SEGMENT_NAME);

  private PinotConfiguration _fetcherConfig;

  @BeforeClass
  public void setUp() {
    _fetcherConfig = new PinotConfiguration();
    _fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
    _fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 10);
    _fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 1.1);
    _fetcherConfig.setProperty(CONNECTION_REQUEST_TIMEOUT_CONFIG_KEY, 1000);
    _fetcherConfig.setProperty(SOCKET_TIMEOUT_CONFIG_KEY, 1000);
  }

  private HttpSegmentFetcher getSegmentFetcher(FileUploadDownloadClient client) {
    HttpSegmentFetcher segmentFetcher;
    if (ThreadLocalRandom.current().nextBoolean()) {
      segmentFetcher = new HttpsSegmentFetcher();
    } else {
      segmentFetcher = new HttpSegmentFetcher();
    }

    segmentFetcher.init(_fetcherConfig);
    segmentFetcher.setHttpClient(client);

    Assert.assertEquals(segmentFetcher.getSocketTimeoutMs(), 1000);
    Assert.assertEquals(segmentFetcher.getConnectionRequestTimeoutMs(), 1000);
    return segmentFetcher;
  }

  @Test
  public void testFetchSegmentToLocalSucceedAtFirstAttempt()
      throws Exception {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    when(client.downloadFile(any(), any(), any())).thenReturn(200);
    HttpSegmentFetcher segmentFetcher = getSegmentFetcher(client);
    List<URI> uris = List.of(new URI("http://h1:8080"), new URI("http://h2:8080"));
    segmentFetcher.fetchSegmentToLocal(SEGMENT_NAME, () -> uris, SEGMENT_FILE);
  }

  @Test(expectedExceptions = AttemptsExceededException.class)
  public void testFetchSegmentToLocalAllDownloadAttemptsFailed()
      throws Exception {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // All attempts failed
    when(client.downloadFile(any(), any(), any())).thenReturn(300);
    HttpSegmentFetcher segmentFetcher = getSegmentFetcher(client);
    List<URI> uris = List.of(new URI("http://h1:8080"), new URI("http://h2:8080"));
    segmentFetcher.fetchSegmentToLocal(SEGMENT_NAME, () -> uris, SEGMENT_FILE);
  }

  @Test
  public void testFetchSegmentToLocalSuccessAfterRetry()
      throws Exception {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // The first two attempts failed and the last attempt succeeded
    when(client.downloadFile(any(), any(), any())).thenReturn(300).thenReturn(300).thenReturn(200);
    HttpSegmentFetcher segmentFetcher = getSegmentFetcher(client);
    List<URI> uris = List.of(new URI("http://h1:8080"), new URI("http://h2:8080"));
    segmentFetcher.fetchSegmentToLocal(SEGMENT_NAME, () -> uris, SEGMENT_FILE);
  }

  @Test
  public void testFetchSegmentToLocalSuccessAfterFirstTwoAttemptsFoundNoPeerServers()
      throws Exception {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // The download always succeeds
    when(client.downloadFile(any(), any(), any())).thenReturn(200);
    HttpSegmentFetcher segmentFetcher = getSegmentFetcher(client);
    List<URI> uris = List.of(new URI("http://h1:8080"), new URI("http://h2:8080"));
    // The first two attempts found NO peers hosting the segment, and the last one found two servers
    //noinspection unchecked
    Supplier<List<URI>> uriSupplier = mock(Supplier.class);
    when(uriSupplier.get()).thenReturn(List.of()).thenReturn(List.of()).thenReturn(uris);
    segmentFetcher.fetchSegmentToLocal(SEGMENT_NAME, uriSupplier, SEGMENT_FILE);
  }

  @Test(expectedExceptions = AttemptsExceededException.class)
  public void testFetchSegmentToLocalFailureWithNoPeerServers()
      throws Exception {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // The download always succeeds
    when(client.downloadFile(any(), any(), any())).thenReturn(200);
    HttpSegmentFetcher segmentFetcher = getSegmentFetcher(client);
    List<URI> uris = List.of();
    segmentFetcher.fetchSegmentToLocal(SEGMENT_NAME, () -> uris, SEGMENT_FILE);
  }
}
