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
package org.apache.pinot.core.metadata;

import java.io.File;
import org.apache.pinot.segment.spi.SegmentMetadata;


/**
 * The metadata extractor will take an input zipped .tar.gz file and extract and return the pinot segment metadata.
 * This class is used during segment upload to get the metadata we need to store in zk.
 */
public interface MetadataExtractor {
  /**
   * Returns a SegmentMetadata object from a tarred file
   * @param tarFile
   * @param workingDir
   * @return
   * @throws Exception
   */
  SegmentMetadata extractMetadata(File tarFile, File workingDir)
      throws Exception;
}
