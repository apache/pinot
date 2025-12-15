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
package org.apache.pinot.controller.helix.core.replication;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.CopyTablePayload;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies a realtime segment from source to destination.
 */
public class RealtimeSegmentCopier implements SegmentCopier {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentCopier.class);
  private static final String SEGMENT_UPLOAD_ENDPOINT_TEMPLATE = "/segments?tableName=%s";

  private final String _destinationDeepStoreUri;
  private final HttpClient _httpClient;

  public RealtimeSegmentCopier(ControllerConf controllerConf) {
    this(controllerConf, HttpClient.getInstance());
  }

  public RealtimeSegmentCopier(ControllerConf controllerConf, HttpClient httpClient) {
    _destinationDeepStoreUri = controllerConf.getDataDir();
    _httpClient = httpClient;
  }


  /**
   * Copies a segment to the destination cluster.
   *
   * This method performs the following steps:
   * 1. Get the source segment URI from ZK metadata.
   * 2. Copy the segment from the source deep store to the destination deep store.
   * 3. Upload the segment to the destination controller.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentName Segment name
   * @param copyTablePayload Payload for copying a table
   * @param segmentZKMetadata ZK metadata for the segment
   */
  @Override
  public void copy(String tableNameWithType, String segmentName, CopyTablePayload copyTablePayload,
      Map<String, String> segmentZKMetadata) {
    if (!tableNameWithType.endsWith("_REALTIME")) {
      throw new IllegalArgumentException("Table name must end with _REALTIME");
    }
    String tableName = tableNameWithType.substring(0, tableNameWithType.lastIndexOf("_REALTIME"));
    try {
      // 1. Get the the source segment uri
      String downloadUrl = segmentZKMetadata.get("segment.download.url");
      if (downloadUrl == null) {
        throw new RuntimeException("Download URL not found in segment ZK metadata for segment: " + segmentName);
      }

      // 2. Copy the segment from the source deep store to the destination deep store
      String destSegmentUriStr = _destinationDeepStoreUri + "/" + tableName + "/" + segmentName;
      LOGGER.info("[copyTable] Copying segment: {} from url: {} to destination: {}", segmentName, downloadUrl,
          destSegmentUriStr);
      URI sourceSegmentUri = new URI(downloadUrl);
      URI destSegmentUri = new URI(destSegmentUriStr);
      PinotFS sourcePinotFS = getPinotFS(sourceSegmentUri);
      PinotFS destPinotFS = getPinotFS(destSegmentUri);

      // TODO: use local file system as an intermediate store to support different file system
      if (sourcePinotFS != destPinotFS) {
        throw new IllegalArgumentException("Copy files across different file system is not supported");
      }

      if (!destPinotFS.exists(destSegmentUri)) {
        if (!destPinotFS.copy(sourceSegmentUri, destSegmentUri)) {
          throw new RuntimeException("Failed to copy segment " + segmentName + " from " + downloadUrl + " to "
              + destSegmentUriStr);
        }
        LOGGER.info("[copyTable] Copied segment {} from {} to {}", segmentName, sourceSegmentUri, destSegmentUri);
      } else {
        LOGGER.info("[copyTable] Segment {} already exists at destination {}", segmentName, destSegmentUri);
      }

      // 3. Upload the segment to the destination controller
      LOGGER.info("[copyTable] Uploading segment {} to destination controller", segmentName);
      String dstControllerURIStr = copyTablePayload.getDestinationClusterUri();
      URI segmentPushURI = FileUploadDownloadClient.getUploadSegmentURI(new URI(dstControllerURIStr));

      // TODO: Refactor SegmentPushUtils.java and FileUploadDownloadClient to dedup code
      RetryPolicies.exponentialBackoffRetryPolicy(1, 5000, 5).attempt(() -> {
        try {
          SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
              _httpClient.sendRequest(
                  getSendSegmentUriRequest(segmentPushURI, destSegmentUriStr,
                      copyTablePayload.getDestinationClusterHeaders(), tableName),
                  HttpClient.DEFAULT_SOCKET_TIMEOUT_MS));
          LOGGER.info("[copyTable] Response for pushing table {} segment uri {} to location {} - {}: {}", tableName,
              destSegmentUriStr, dstControllerURIStr, response.getStatusCode(),
              response.getResponse());
          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode >= 500) {
            // Temporary exception
            LOGGER.warn("[copyTable] Caught temporary error when pushing table: {} segment uri: {} to {}, will retry",
                tableName, destSegmentUriStr, dstControllerURIStr, e);
            return false;
          } else {
            // Permanent exception
            LOGGER.error("[copyTable] Caught permanent error when pushing table: {} segment uri: {} to {}, won't retry",
                tableName, destSegmentUriStr, dstControllerURIStr, e);
            throw e;
          }
        }
      });
    } catch (Exception e) {
      LOGGER.error("[copyTable] Caught exception while copying segment {}", segmentName, e);
      throw new RuntimeException(e);
    }
  }

  static ClassicHttpRequest getSendSegmentUriRequest(URI uri, String downloadUri,
      Map<String, String> headers, String tableNameWithoutType) {
    ClassicRequestBuilder requestBuilder = ClassicRequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(
            FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE, FileUploadDownloadClient.FileUploadType.URI.toString())
        .setHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI, downloadUri)
        .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE);

    for (Map.Entry<String, String> pair: headers.entrySet()) {
      requestBuilder.setHeader(pair.getKey(), pair.getValue());
    }

    HttpClient.addHeadersAndParameters(requestBuilder, Collections.emptyList(), Collections.singletonList(
        new BasicNameValuePair("tableName", tableNameWithoutType)));
    return requestBuilder.build();
  }

  static String getScheme(URI uri) {
    if (uri.getScheme() != null) {
      return uri.getScheme();
    }
    return PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
  }

  PinotFS getPinotFS(URI uri) {
    String scheme = getScheme(uri);
    if (!PinotFSFactory.isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("File scheme " + scheme + " is not supported.");
    }
    return PinotFSFactory.create(scheme);
  }
}
