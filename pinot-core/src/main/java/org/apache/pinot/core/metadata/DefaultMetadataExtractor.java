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
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;


/**
 * DefaultMetadataExtractor is an implementation of the MetadataExtractor interface.
 * By default, the metadata extractor we will use will assume that we are provided a .tar.gz pinot segment file.
 */
public class DefaultMetadataExtractor implements MetadataExtractor {

  static final String SEGMENT_METADATA_DIR_NAME = "segment_metadata";

  @Override
  public SegmentMetadata extractMetadata(File tarredSegmentFile, File unzippedSegmentDir)
      throws Exception {
    // NOTE: While there is TarGzCompressionUtils.untarOneFile(), we use untar() here to unpack all files in the segment
    //       in order to ensure the segment is not corrupted.
    List<File> untarredFiles = TarCompressionUtils.untar(tarredSegmentFile, unzippedSegmentDir);
    File indexDir = untarredFiles.get(0);
    if (indexDir.isDirectory()) {
      return new SegmentMetadataImpl(indexDir);
    } else {
      // If the first file is not a directory, create a subdirectory and move all files into it
      File segmentMetadataDir = new File(unzippedSegmentDir, SEGMENT_METADATA_DIR_NAME);
      if (!segmentMetadataDir.exists() && !segmentMetadataDir.mkdirs()) {
        throw new IOException("Failed to create segment_metadata directory: " + segmentMetadataDir.getAbsolutePath());
      }
      for (File file : untarredFiles) {
        File targetFile = new File(segmentMetadataDir, file.getName());
        if (file.isDirectory()) {
          FileUtils.moveDirectory(file, targetFile);
        } else {
          FileUtils.moveFile(file, targetFile);
        }
      }
      return new SegmentMetadataImpl(segmentMetadataDir);
    }
  }
}
