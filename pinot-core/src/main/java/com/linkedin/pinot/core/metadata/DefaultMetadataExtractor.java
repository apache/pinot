/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.metadata;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DefaultMetadataExtractor is an implementation of the MetadataExtractor interface.
 * By default, the metadata extractor we will use will assume that we are provided a .tar.gz pinot segment file.
 */
public class DefaultMetadataExtractor implements MetadataExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataExtractor.class);

  @Override
  public SegmentMetadata extractMetadata(File tarredSegmentFile, File unzippedSegmentDir) throws Exception {
    // While there is TarGzCompressionUtils.unTarOneFile, we use unTar here to unpack all files
    // in the segment in order to ensure the segment is not corrupted
    List<File> files = TarGzCompressionUtils.unTar(tarredSegmentFile, unzippedSegmentDir);
//    File[] files = unzippedSegmentDir.listFiles();
    Preconditions.checkState(files != null && files.get(0).isDirectory());
    System.out.println("File name: " + files.get(0).getName() + " Absolute path: " + files.get(0).getAbsolutePath());
    LOGGER.info("File name: " + files.get(0).getName() + " Absolute path: " + files.get(0).getAbsolutePath());
    File indexDir = files.get(0);
    return new SegmentMetadataImpl(indexDir);
  }

}
