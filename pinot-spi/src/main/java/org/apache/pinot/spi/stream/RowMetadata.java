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
package org.apache.pinot.spi.stream;

import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * A class that provides relevant row-level metadata for rows indexed into a segment.
 *
 * Currently this is relevant for rows ingested into a mutable segment - the metadata is expected to be
 * provided by the underlying stream.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RowMetadata {

  /**
   * Return the timestamp associated with when the row was ingested upstream.
   * Expected to be mainly used for stream-based sources.
   *
   * @return timestamp (epoch in milliseconds) when the row was ingested upstream
   *         Long.MIN_VALUE if not available
   */
  long getIngestionTimeMs();
}
