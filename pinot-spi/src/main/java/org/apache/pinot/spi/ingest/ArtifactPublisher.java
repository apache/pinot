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
package org.apache.pinot.spi.ingest;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Manages the staging and publishing of segment artifacts (e.g., segment tar files) produced by
 * INSERT INTO statements.
 *
 * <p>Artifacts are first staged to a temporary location, then atomically published (moved to the
 * final deep-store location) upon commit. If the statement is aborted, staged artifacts are
 * cleaned up.
 *
 * <p>Implementations must be thread-safe for concurrent operations across different statements.
 */
public interface ArtifactPublisher {

  /**
   * Initializes the publisher with the given configuration. Called once during startup.
   *
   * @param config the Pinot configuration
   */
  void init(PinotConfiguration config);

  /**
   * Stages an artifact for the given statement. The artifact is written to a temporary location
   * and a URI is returned for later reference.
   *
   * @param statementId  the statement identifier
   * @param artifactName the name of the artifact (e.g., segment file name)
   * @param data         the artifact data stream
   * @return the URI of the staged artifact
   */
  URI stageArtifact(String statementId, String artifactName, InputStream data);

  /**
   * Publishes previously staged artifacts, moving them to their final locations.
   *
   * @param statementId  the statement identifier
   * @param artifactUris the URIs of the staged artifacts to publish
   */
  void publishArtifacts(String statementId, List<URI> artifactUris);

  /**
   * Cleans up all staged artifacts for the given statement. Called on abort or after successful
   * publish to remove temporary files.
   *
   * @param statementId the statement identifier
   */
  void cleanupArtifacts(String statementId);
}
