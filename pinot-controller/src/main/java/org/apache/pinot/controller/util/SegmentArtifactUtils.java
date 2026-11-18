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

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.URIUtils;


/// Stateless helpers for identifying immutable realtime artifacts owned by the Controller's configured deep store.
public final class SegmentArtifactUtils {
  private static final Pattern IMMUTABLE_ARTIFACT = Pattern.compile(
      "(.+)\\.(?:commit|recovery)\\.[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

  private SegmentArtifactUtils() {
  }

  @Nullable
  public static String getLogicalSegmentName(String fileName) {
    Matcher matcher = IMMUTABLE_ARTIFACT.matcher(fileName);
    return matcher.matches() ? matcher.group(1) : null;
  }

  @Nullable
  public static String getReferencedFileName(SegmentZKMetadata metadata) {
    String downloadUrl = metadata.getDownloadUrl();
    if (StringUtils.isBlank(downloadUrl)) {
      return null;
    }
    URI uri = URIUtils.getUri(downloadUrl);
    String path = uri.getRawPath();
    return path != null ? URIUtils.decode(path.substring(path.lastIndexOf('/') + 1)) : null;
  }

  /// Returns an owned deep-store URI, never an external download URL. HTTP URLs must match the trusted Controller VIP.
  @Nullable
  public static URI getOwnedArtifactUri(String dataDir, String rawTableName, SegmentZKMetadata metadata,
      @Nullable String controllerVipUrl) {
    if (!metadata.isReplicateMode()) {
      return null;
    }
    String fileName = getReferencedFileName(metadata);
    if (fileName == null || !metadata.getSegmentName().equals(getLogicalSegmentName(fileName))) {
      return null;
    }
    return resolveOwnedArtifactUri(dataDir, rawTableName, metadata.getDownloadUrl(), controllerVipUrl);
  }

  /// Resolves references from either typed table, including aliases whose logical name differs from the artifact.
  @Nullable
  public static URI resolveOwnedArtifactUri(String dataDir, String rawTableName, String downloadUrl,
      @Nullable String controllerVipUrl) {
    if (StringUtils.isBlank(downloadUrl)) {
      return null;
    }
    URI downloadUri = URIUtils.getUri(downloadUrl);
    String path = downloadUri.getRawPath();
    String fileName = path != null ? URIUtils.decode(path.substring(path.lastIndexOf('/') + 1)) : null;
    if (fileName == null || getLogicalSegmentName(fileName) == null) {
      return null;
    }
    URI ownedUri = URIUtils.getUri(dataDir, rawTableName, URIUtils.encode(fileName));
    if ("http".equalsIgnoreCase(downloadUri.getScheme()) || "https".equalsIgnoreCase(downloadUri.getScheme())) {
      URI expectedUri = StringUtils.isNotBlank(controllerVipUrl)
          ? URIUtils.getUri(URIUtils.constructDownloadUrl(controllerVipUrl, rawTableName, fileName))
          : null;
      return "file".equalsIgnoreCase(ownedUri.getScheme()) && expectedUri != null
          && expectedUri.normalize().equals(downloadUri.normalize())
          ? ownedUri
          : null;
    }
    return ownedUri.normalize().equals(downloadUri.normalize()) ? ownedUri : null;
  }
}
