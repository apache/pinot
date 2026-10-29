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
package org.apache.pinot.spi.ingestion.error;

/// What failed when Pinot accessed an ingestion source (a stream, its schema registry, a file system). The category
/// never depends on whose credential failed or who can fix it, which Pinot can't know.
///
/// A failure that is not a failure to access the source has no category at all: Pinot's own cancellation, content
/// that was read but can't be parsed, or an exception no rule recognises.
public enum IngestionSourceErrorCategory {
  /// Authentication or authorization failed: a credential, signature or token was rejected, a permission or an access
  /// policy denied the request, the source answered "does not exist or not authorized", a credential couldn't be
  /// loaded, read or built, or an identity service refused the request.
  AUTH,
  /// Any other failure to read from the source: the data is missing or expired, the source throttled or failed after
  /// our retries, it rejected a request as invalid, the client couldn't be set up, or an endpoint, an identity
  /// service's included, never answered.
  SOURCE_READ
}
