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
package org.apache.pinot.controller.helix.core.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingest.ArtifactPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages staging and publishing of segment artifacts produced by file-based INSERT INTO statements.
 *
 * <p>Artifacts are staged in a temporary namespace within the deep store (under a
 * {@code _insert_staging/<statementId>/} prefix). On commit, they are moved to their final
 * location. On abort, staged artifacts are cleaned up.
 *
 * <p>This publisher delegates to the configured {@link PinotFS} implementation for the deep store
 * scheme.
 *
 * <p>This class is thread-safe for concurrent operations across different statements.
 */
public class FileInsertArtifactPublisher implements ArtifactPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileInsertArtifactPublisher.class);

  static final String STAGING_DIR_PREFIX = "_insert_staging";
  static final String DEEP_STORE_URI_KEY = "controller.data.dir";

  private URI _deepStoreBaseUri;

  /**
   * Tracks which statements have staged artifacts so cleanup can enumerate them.
   */
  private final ConcurrentHashMap<String, Set<URI>> _stagedArtifacts = new ConcurrentHashMap<>();

  @Override
  public void init(PinotConfiguration config) {
    String deepStoreUri = config.getProperty(DEEP_STORE_URI_KEY);
    if (deepStoreUri != null) {
      _deepStoreBaseUri = URIUtils.getUri(deepStoreUri);
    }
  }

  /**
   * Sets the deep store base URI directly. Useful for testing.
   */
  public void setDeepStoreBaseUri(URI deepStoreBaseUri) {
    _deepStoreBaseUri = URIUtils.getUri(deepStoreBaseUri.toString());
  }

  @Override
  public URI stageArtifact(String statementId, String artifactName, InputStream data) {
    URI stagingDir = getStagingDir(statementId);
    URI artifactUri = URIUtils.getUri(stagingDir.toString(), artifactName);

    try {
      PinotFS pinotFS = PinotFSFactory.create(stagingDir.getScheme());
      pinotFS.mkdir(stagingDir);
      // PinotFS requires a File, so write InputStream to a temp file first
      java.io.File tempFile = java.io.File.createTempFile("insert-stage-", "-" + artifactName);
      try {
        java.nio.file.Files.copy(data, tempFile.toPath(),
            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        pinotFS.copyFromLocalFile(tempFile, artifactUri);
      } finally {
        tempFile.delete();
      }
      LOGGER.info("Staged artifact {} for statement {}", artifactUri, statementId);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to stage artifact " + artifactName + " for statement " + statementId, e);
    }

    _stagedArtifacts.computeIfAbsent(statementId, k -> ConcurrentHashMap.newKeySet()).add(artifactUri);
    return artifactUri;
  }

  @Override
  public void publishArtifacts(String statementId, List<URI> artifactUris) {
    for (URI artifactUri : artifactUris) {
      // The actual publishing (moving from staging to final location) is handled by the
      // PublishCoordinator via endReplaceSegments. This method serves as a hook for any
      // additional deep-store file moves if needed.
      LOGGER.info("Publishing artifact {} for statement {}", artifactUri, statementId);
    }
    // Clean up staging tracking after publish
    _stagedArtifacts.remove(statementId);
  }

  @Override
  public void cleanupArtifacts(String statementId) {
    URI stagingDir = getStagingDir(statementId);
    try {
      PinotFS pinotFS = PinotFSFactory.create(stagingDir.getScheme());
      if (pinotFS.exists(stagingDir)) {
        pinotFS.delete(stagingDir, true);
        LOGGER.info("Cleaned up staging directory {} for statement {}", stagingDir, statementId);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to clean up staging directory {} for statement {}", stagingDir, statementId, e);
    }
    _stagedArtifacts.remove(statementId);
  }

  /**
   * Returns the staging directory URI for a given statement.
   */
  URI getStagingDir(String statementId) {
    if (_deepStoreBaseUri == null) {
      throw new IllegalStateException("Deep store base URI is not configured");
    }
    return URIUtils.getUri(_deepStoreBaseUri.toString(), STAGING_DIR_PREFIX, statementId);
  }

  /**
   * Returns the set of staged artifact URIs for a statement, visible for testing.
   */
  Set<URI> getStagedArtifacts(String statementId) {
    return _stagedArtifacts.get(statementId);
  }
}
