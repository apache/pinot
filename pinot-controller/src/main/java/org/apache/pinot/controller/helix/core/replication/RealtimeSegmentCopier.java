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
import java.util.Map;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.CopyTablePayload;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
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
      URI sourceSegmentUri = new URI(downloadUrl);
      PinotFS sourcePinotFS = getPinotFS(sourceSegmentUri);
      String destSegmentUriStr = _destinationDeepStoreUri + "/" + tableName + "/" + segmentName;
      URI destSegmentUri = new URI(destSegmentUriStr);

      PinotFS destPinotFS = getPinotFS(destSegmentUri);

      // TODO: use local file system as an intermediate store to support different file system
      if (sourcePinotFS != destPinotFS) {
        throw new IllegalArgumentException("Copy files across different file system is not supported");
      }

      if (!destPinotFS.exists(destSegmentUri)) {
        sourcePinotFS.copy(sourceSegmentUri, destSegmentUri);
        LOGGER.info("Copied segment {} from {} to {}", segmentName, sourceSegmentUri, destSegmentUri);
      } else {
        LOGGER.info("Segment {} already exists at destination {}", segmentName, destSegmentUri);
      }

      // 3. Upload the segment to the destination controller
      String payload = "{\"segmentUri\":\"" + destSegmentUriStr + "\"}";
      URI uri = new URI(copyTablePayload.getDestinationClusterUri() + String.format(SEGMENT_UPLOAD_ENDPOINT_TEMPLATE,
          tableName));
      SimpleHttpResponse response =
          _httpClient.sendJsonPostRequest(uri, payload, copyTablePayload.getDestinationClusterHeaders());
      LOGGER.info("Uploaded segment {} to destination controller, status: {}", segmentName, response.getStatusCode());
    } catch (Exception e) {
      LOGGER.error("Caught exception while copying segment {}", segmentName, e);
      throw new RuntimeException(e);
    }
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
