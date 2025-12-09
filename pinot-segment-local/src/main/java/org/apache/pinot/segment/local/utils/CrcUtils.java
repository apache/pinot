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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import org.apache.pinot.segment.spi.V1Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("Duplicates")
public class CrcUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrcUtils.class);
  private static final int BUFFER_SIZE = 65536;
  private static final String CRC_FILE_EXTENSTION = ".crc";
  private static final List<String> DATA_FILE_EXTENSIONS = Arrays.asList(".fwd", ".dict");

  private final List<File> _files;
  private final List<File> _dataFiles;

  private CrcUtils(List<File> files, List<File> dataFiles) {
    _files = files;
    _dataFiles = dataFiles;
  }

  public static CrcUtils forAllFilesInFolder(File dir) {
    List<File> allNormalFiles = new ArrayList<>();
    List<File> dataFiles = new ArrayList<>();
    collectFiles(dir, allNormalFiles, dataFiles);
    Collections.sort(allNormalFiles);
    Collections.sort(dataFiles);
    return new CrcUtils(allNormalFiles, dataFiles);
  }

  /**
   * Helper method to get all files (normal and data files) in the directory to later compute CRC for them.
   * <p>NOTE: do not include the segment creation meta file.
   */
  private static void collectFiles(File dir, List<File> normalFiles, List<File> dataFiles) {
    File[] files = dir.listFiles();
    Preconditions.checkNotNull(files);
    for (File file : files) {
      if (file.isFile()) {
        String fileName = file.getName();
        // Certain file systems, e.g. HDFS will create .crc files when perform data copy.
        // We should ignore both SEGMENT_CREATION_META and generated '.crc' files.
        if (!fileName.equals(V1Constants.SEGMENT_CREATION_META) && !fileName
            .endsWith(CRC_FILE_EXTENSTION)) {
          // add all files to normal files
          normalFiles.add(file);
          //include data extension files to dataFiles
          // Conditionally add to the data-only list
          if (isDataFile(fileName)) {
            dataFiles.add(file);
          }
        }
      } else {
        collectFiles(file, normalFiles, dataFiles);
      }
    }
  }

  /**
   * Determines if a file is considered a "Data File" (one of ".fwd", ".dict" file types).
   */
  private static boolean isDataFile(String fileName) {
    for (String ext : DATA_FILE_EXTENSIONS) {
      if (fileName.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  public long computeCrc() throws IOException {
    return crcForFiles(_files);
  }

  public long computeDataCrc() throws IOException {
    return crcForFiles(_dataFiles);
  }

  private long crcForFiles(List<File> filesToComputeCrc) throws IOException {
    byte[] buffer = new byte[BUFFER_SIZE];
    Checksum checksum = new Adler32();

    for (File file : filesToComputeCrc) {
      try (InputStream input = new FileInputStream(file)) {
        int len;
        while ((len = input.read(buffer)) > 0) {
          checksum.update(buffer, 0, len);
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Updated crc = {}, based on file {} of length {}", checksum.getValue(), file, file.length());
        }
      }
    }
    long crc = checksum.getValue();
    LOGGER.info("Computed crc = {}, based on files {}", crc, filesToComputeCrc);
    return crc;
  }
}
