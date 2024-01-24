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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.PeerServerSegmentFinder;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;


public class HttpSegmentFetcherTest {
  private MockedStatic<PeerServerSegmentFinder> peerServerSegmentFinder = mockStatic(PeerServerSegmentFinder.class);
  private PinotConfiguration fetcherConfig;

  @BeforeSuite
  public void initTest() {
    fetcherConfig = new PinotConfiguration();
    fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
  }

  @Test
  public void testFetchSegmentToLocalSucceedAtFirstAttempt()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    when(client.downloadFile(any(), any(), any())).thenReturn(200);
    HttpSegmentFetcher httpSegmentFetcher = new HttpSegmentFetcher(client, fetcherConfig);
    HelixManager helixManager = mock(HelixManager.class);

    List<URI> uris = new ArrayList<>();
    uris.add(new URI("http://h1:8080"));
    uris.add(new URI("http://h2:8080"));
    peerServerSegmentFinder.when(() -> PeerServerSegmentFinder.getPeerServerURIs(any(), any(), any())).thenReturn(uris);
    try {
      Assert.assertTrue(httpSegmentFetcher.fetchSegmentToLocal("seg", new File("/file"), helixManager, "http"));
    } catch (Exception e) {
      Assert.assertTrue(false, "Download segment failed");
    }
    peerServerSegmentFinder.reset();
  }

  @Test
  public void testFetchSegmentToLocalAllDownloadAttemptsFailed()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // All three attempts fails.
    when(client.downloadFile(any(), any(), any())).thenReturn(300).thenReturn(300).thenReturn(300);
    HttpSegmentFetcher httpSegmentFetcher = new HttpSegmentFetcher(client, fetcherConfig);
    HelixManager helixManager = mock(HelixManager.class);
    List<URI> uris = new ArrayList<>();
    uris.add(new URI("http://h1:8080"));
    uris.add(new URI("http://h2:8080"));

    peerServerSegmentFinder.when(() -> PeerServerSegmentFinder.getPeerServerURIs(any(), any(), any())).thenReturn(uris);
    try {
      Assert.assertFalse(httpSegmentFetcher.fetchSegmentToLocal("seg", new File("/file"), helixManager, "http"));
    } catch (Exception e) {
      Assert.assertTrue(false, "Download segment failed");
    }
  }

  @Test
  public void testFetchSegmentToLocalSuccessAfterRetry()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // the first two attempts failed until the last attempt succeeds
    when(client.downloadFile(any(), any(), any())).thenReturn(300).thenReturn(300).thenReturn(200);
    HttpSegmentFetcher httpSegmentFetcher = new HttpSegmentFetcher(client, fetcherConfig);
    HelixManager helixManager = mock(HelixManager.class);
    List<URI> uris = new ArrayList<>();
    uris.add(new URI("http://h1:8080"));
    uris.add(new URI("http://h2:8080"));

    peerServerSegmentFinder.when(() -> PeerServerSegmentFinder.getPeerServerURIs(any(), any(), any())).thenReturn(uris);
    try {
      Assert.assertTrue(httpSegmentFetcher.fetchSegmentToLocal("seg", new File("/file"), helixManager, "http"));
    } catch (Exception e) {
      Assert.assertTrue(false, "Download segment failed");
    }
  }

  @Test
  public void testFetchSegmentToLocalSuccessAfterFirstTwoAttemptsFoundNoPeerServers()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    //  The download always succeeds.
    when(client.downloadFile(any(), any(), any())).thenReturn(200);
    HttpSegmentFetcher httpSegmentFetcher = new HttpSegmentFetcher(client, fetcherConfig);
    HelixManager helixManager = mock(HelixManager.class);
    List<URI> uris = new ArrayList<>();
    uris.add(new URI("http://h1:8080"));
    uris.add(new URI("http://h2:8080"));

    // The first two attempts find NO peers hosting the segment but the last one found two servers.
    peerServerSegmentFinder.when(() -> PeerServerSegmentFinder.getPeerServerURIs(any(), any(), any())).
        thenReturn(List.of()).
        thenReturn(List.of()).
        thenReturn(uris);
    try {
      Assert.assertTrue(httpSegmentFetcher.fetchSegmentToLocal("seg", new File("/file"), helixManager, "http"));
    } catch (Exception e) {
      Assert.assertTrue(false, "Download segment failed");
    }
  }

  @Test
  public void testFetchSegmentToLocalFailureAfterNoPeerServers()
      throws IOException, HttpErrorStatusException {
    FileUploadDownloadClient client = mock(FileUploadDownloadClient.class);
    // the download always succeeds.
    when(client.downloadFile(any(), any(), any())).thenReturn(200);
    HttpSegmentFetcher httpSegmentFetcher = new HttpSegmentFetcher(client, fetcherConfig);
    HelixManager helixManager = mock(HelixManager.class);

    peerServerSegmentFinder.when(() -> PeerServerSegmentFinder.getPeerServerURIs(any(), any(), any())).
        thenReturn(List.of()).
        thenReturn(List.of()).
        thenReturn(List.of());
    try {
      Assert.assertFalse(httpSegmentFetcher.fetchSegmentToLocal("seg", new File("/file"), helixManager, "http"));
    } catch (Exception e) {
      Assert.assertTrue(false, "Download segment failed");
    }
  }
}
