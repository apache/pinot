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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.http.HttpStatus;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.common.auth.UrlAuthProvider;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_TOKEN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@Test
public class CursorWithAuthIntegrationTest extends CursorIntegrationTest {
  final static String AUTH_PROVIDER_CLASS = UrlAuthProvider.class.getCanonicalName();
  final static URL AUTH_URL = CursorWithAuthIntegrationTest.class.getResource("/url-auth-token.txt");
  final static String AUTH_PREFIX = "Basic";

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    BasicAuthTestUtils.addControllerConfiguration(properties);
    properties.put("controller.segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    properties.put("controller.segment.fetcher.auth.url", AUTH_URL);
    properties.put("controller.segment.fetcher.auth.prefix", AUTH_PREFIX);
    properties.put("controller.cluster.response.store.auth.provider.class", AUTH_PROVIDER_CLASS);
    properties.put("controller.cluster.response.store.auth.url", AUTH_URL);
    properties.put("controller.cluster.response.store.auth.prefix", AUTH_PREFIX);
    properties.put(CommonConstants.CursorConfigs.RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD, "5m");
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    super.overrideBrokerConf(configuration);
    BasicAuthTestUtils.addBrokerConfiguration(configuration);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);
    serverConf.setProperty("pinot.server.segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    serverConf.setProperty("pinot.server.segment.fetcher.auth.url", AUTH_URL);
    serverConf.setProperty("pinot.server.segment.fetcher.auth.prefix", AUTH_PREFIX);
    serverConf.setProperty("pinot.server.segment.uploader.auth.provider.class", AUTH_PROVIDER_CLASS);
    serverConf.setProperty("pinot.server.segment.uploader.auth.url", AUTH_URL);
    serverConf.setProperty("pinot.server.segment.uploader.auth.prefix", AUTH_PREFIX);
    serverConf.setProperty("pinot.server.instance.auth.provider.class", AUTH_PROVIDER_CLASS);
    serverConf.setProperty("pinot.server.instance.auth.url", AUTH_URL);
    serverConf.setProperty("pinot.server.instance.auth.prefix", AUTH_PREFIX);
  }

  @Override
  protected Map<String, String> getHeaders() {
    return BasicAuthTestUtils.AUTH_HEADER;
  }

  @Override
  public ControllerRequestClient getControllerRequestClient() {
    if (_controllerRequestClient == null) {
      _controllerRequestClient =
          new ControllerRequestClient(_controllerRequestURLBuilder, getHttpClient(), AUTH_HEADER);
    }
    return _controllerRequestClient;
  }

  @Override
  protected Connection getPinotConnection() {
    if (_pinotConnection == null) {
      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
      factory.setHeaders(AUTH_HEADER);

      _pinotConnection =
          ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
    }
    return _pinotConnection;
  }

  /**
   * Upload all segments inside the given directories to the cluster.
   */
  @Override
  protected void uploadSegments(String tableName, TableType tableType, List<File> tarDirs)
      throws Exception {
    List<File> segmentTarFiles = new ArrayList<>();
    for (File tarDir : tarDirs) {
      File[] tarFiles = tarDir.listFiles();
      assertNotNull(tarFiles);
      Collections.addAll(segmentTarFiles, tarFiles);
    }
    int numSegments = segmentTarFiles.size();
    assertTrue(numSegments > 0);

    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    NameValuePair
        tableNameValuePair = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE,
        tableType.name());
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair, tableTypeValuePair);
    List<Header> headers = List.of(new BasicHeader("Authorization", AUTH_TOKEN));

    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles.get(0);
        if (System.currentTimeMillis() % 2 == 0) {
          assertEquals(
              fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
                  headers, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode(), HttpStatus.SC_OK);
        } else {
          assertEquals(
              uploadSegmentWithOnlyMetadata(tableName, tableType, uploadSegmentHttpURI, fileUploadDownloadClient,
                  segmentTarFile), HttpStatus.SC_OK);
        }
      } else {
        // Upload all segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(() -> {
            if (System.currentTimeMillis() % 2 == 0) {
              return fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(),
                  segmentTarFile, headers, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode();
            } else {
              return uploadSegmentWithOnlyMetadata(tableName, tableType, uploadSegmentHttpURI, fileUploadDownloadClient,
                  segmentTarFile);
            }
          }));
        }
        executorService.shutdown();
        for (Future<Integer> future : futures) {
          assertEquals((int) future.get(), HttpStatus.SC_OK);
        }
      }
    }
  }

  private int uploadSegmentWithOnlyMetadata(String tableName, TableType tableType, URI uploadSegmentHttpURI,
      FileUploadDownloadClient fileUploadDownloadClient, File segmentTarFile)
      throws IOException, HttpErrorStatusException {
    List<Header> headers = List.of(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI,
            String.format("file://%s/%s", segmentTarFile.getParentFile().getAbsolutePath(),
                URIUtils.encode(segmentTarFile.getName()))),
        new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
            FileUploadDownloadClient.FileUploadType.METADATA.toString()),
        new BasicHeader("Authorization", AUTH_TOKEN));
    // Add table name and table type as request parameters
    NameValuePair tableNameValuePair =
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair =
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, tableType.name());
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair, tableTypeValuePair);
    return fileUploadDownloadClient.uploadSegmentMetadata(uploadSegmentHttpURI, segmentTarFile.getName(),
        segmentTarFile, headers, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode();
  }
}
