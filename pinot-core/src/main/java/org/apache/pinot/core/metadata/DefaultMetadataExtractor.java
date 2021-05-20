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
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.SegmentMetadata;


/**
 * DefaultMetadataExtractor is an implementation of the MetadataExtractor interface.
 * By default, the metadata extractor we will use will assume that we are provided a .tar.gz pinot segment file.
 */
public class DefaultMetadataExtractor implements MetadataExtractor {

  @Override
  public SegmentMetadata extractMetadata(File tarredSegmentFile, File unzippedSegmentDir)
      throws Exception {
    // NOTE: While there is TarGzCompressionUtils.untarOneFile(), we use untar() here to unpack all files in the segment
    //       in order to ensure the segment is not corrupted.
    File indexDir = TarGzCompressionUtils.untar(tarredSegmentFile, unzippedSegmentDir).get(0);
    return new SegmentMetadataImpl(indexDir);
  }
}
