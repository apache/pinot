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
package org.apache.pinot.segment.spi.loader;

import java.net.URI;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Interface for creating and loading the {@link SegmentDirectory} instance using provided config
 */
public interface SegmentDirectoryLoader {

  /**
   * Creates the {@link SegmentDirectory} instance
   * @param indexDir index directory
   * @param segmentDirectoryConfig config for SegmentDirectory, containing all properties needed to instantiate the
   *                               {@link SegmentDirectory}
   *                               e.g. readMode (MMAP/HEAP) or
   *                               properties specific to the tier backend (deep store access configs)
   */
  SegmentDirectory load(URI indexDir, PinotConfiguration segmentDirectoryConfig)
      throws Exception;
}
