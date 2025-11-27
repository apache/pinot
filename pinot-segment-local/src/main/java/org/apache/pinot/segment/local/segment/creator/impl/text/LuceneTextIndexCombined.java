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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to combine all Lucene text index files into a single buffer in V2 format.
 * This class handles the serialization of Lucene index directory into a compact buffer format
 * that can be efficiently stored and loaded.
 *
 * <p>The V2 format structure:</p>
 * <pre>
 * [Header Section]
 * - Magic number: "LUCENE_V2" (9 bytes)
 * - Version: 2 (4 bytes)
 * - Total buffer size: 8 bytes
 * - File count: 4 bytes
 * - Reserved: 4 bytes (for future use)
 *
 * [File Metadata Section]
 * - File name length: 2 bytes
 * - File name: variable length
 * - File offset: 8 bytes
 * - File size: 8 bytes
 *
 * [File Data Section]
 * - Raw file data concatenated in order
 * </pre>
 */
public class LuceneTextIndexCombined {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexCombined.class);

  /**
   * Private constructor to prevent instantiation of utility class.
   */
  private LuceneTextIndexCombined() {
  }

  /**
   * Combines all files from a Lucene text index directory into a single file.
   *
   * @param luceneIndexDir the Lucene index directory to combine
   * @param outputFilePath the output file path to write the combined data
   * @throws IOException if any file operations fail
   */
  public static void combineLuceneIndexFiles(File luceneIndexDir, String outputFilePath)
      throws IOException {
    if (!luceneIndexDir.exists() || !luceneIndexDir.isDirectory()) {
      throw new IllegalArgumentException(
          "Lucene index directory does not exist or is not a directory: " + luceneIndexDir);
    }

    LOGGER.info("Combining Lucene text index files from directory: {}", luceneIndexDir.getAbsolutePath());

    // Step 1: Collect all files and calculate total size
    Map<String, FileInfo> fileInfoMap = collectFiles(luceneIndexDir);
    int fileCount = fileInfoMap.size();

    if (fileCount == 0) {
      throw new IOException("No files found in Lucene index directory: " + luceneIndexDir);
    }

    // Step 2: Calculate total buffer size
    long totalSize = calculateTotalBufferSize(fileInfoMap);

    if (totalSize > Integer.MAX_VALUE) {
      throw new IOException("Combined index size too large: " + totalSize + " bytes");
    }

    // Step 3: Create output file and write data using FileChannel
    File outputFile = new File(outputFilePath);
    try (FileChannel outputChannel = FileChannel.open(outputFile.toPath(), StandardOpenOption.CREATE,
        StandardOpenOption.WRITE)) {

      // Write header
      writeHeader(outputChannel, fileCount, (int) totalSize);

      // Write file metadata
      long dataOffset = LuceneCombinedTextIndexConstants.getHeaderSize() + calculateMetadataSize(fileInfoMap);
      writeFileMetadata(outputChannel, fileInfoMap, dataOffset);

      // Write file data
      writeFileData(outputChannel, fileInfoMap);
    }

    LOGGER.info("Successfully combined {} files into file: {} (size: {} bytes)", fileCount, outputFilePath, totalSize);
  }

  /**
   * Collects all files from the Lucene index directory and their metadata.
   */
  private static Map<String, FileInfo> collectFiles(File luceneIndexDir)
      throws IOException {
    Map<String, FileInfo> fileInfoMap = new TreeMap<>(); // Use TreeMap for consistent ordering

    File[] files = luceneIndexDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          String fileName = file.getName();
          long fileSize = file.length();

          fileInfoMap.put(fileName, new FileInfo(file, fileName, fileSize));
        }
      }
    }

    return fileInfoMap;
  }

  /**
   * Calculates the total buffer size needed.
   */
  private static long calculateTotalBufferSize(Map<String, FileInfo> fileInfoMap) {
    long totalSize = LuceneCombinedTextIndexConstants.getHeaderSize();
    totalSize += calculateMetadataSize(fileInfoMap);

    for (FileInfo fileInfo : fileInfoMap.values()) {
      totalSize += fileInfo._size;
    }

    return totalSize;
  }

  /**
   * Calculates the size needed for file metadata section.
   */
  private static long calculateMetadataSize(Map<String, FileInfo> fileInfoMap) {
    long metadataSize = 0;
    for (FileInfo fileInfo : fileInfoMap.values()) {
      metadataSize += LuceneCombinedTextIndexConstants.getFileMetadataEntrySizeWithFilename(fileInfo._name);
    }
    return metadataSize;
  }

  /**
   * Writes the header section to the file.
   */
  private static void writeHeader(FileChannel outputChannel, int fileCount, int totalSize)
      throws IOException {
    // Magic number
    outputChannel.write(ByteBuffer.wrap(LuceneCombinedTextIndexConstants.MAGIC_NUMBER.getBytes()));

    // Version
    ByteBuffer versionBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    versionBuffer.putInt(LuceneCombinedTextIndexConstants.VERSION);
    versionBuffer.flip();
    outputChannel.write(versionBuffer);

    // Total buffer size
    ByteBuffer totalSizeBuffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    totalSizeBuffer.putLong(totalSize);
    totalSizeBuffer.flip();
    outputChannel.write(totalSizeBuffer);

    // File count
    ByteBuffer fileCountBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    fileCountBuffer.putInt(fileCount);
    fileCountBuffer.flip();
    outputChannel.write(fileCountBuffer);

    // Reserved
    ByteBuffer reservedBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    reservedBuffer.putInt(0);
    reservedBuffer.flip();
    outputChannel.write(reservedBuffer);
  }

  /**
   * Writes the file metadata section to the file.
   */
  private static void writeFileMetadata(FileChannel outputChannel, Map<String, FileInfo> fileInfoMap, long dataOffset)
      throws IOException {
    for (FileInfo fileInfo : fileInfoMap.values()) {
      // File name length
      ByteBuffer nameLengthBuffer = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      nameLengthBuffer.putShort((short) fileInfo._name.length());
      nameLengthBuffer.flip();
      outputChannel.write(nameLengthBuffer);

      // File name (variable length)
      outputChannel.write(ByteBuffer.wrap(fileInfo._name.getBytes()));

      // File offset
      ByteBuffer fileOffsetBuffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      fileOffsetBuffer.putLong(dataOffset);
      fileOffsetBuffer.flip();
      outputChannel.write(fileOffsetBuffer);

      // File size
      ByteBuffer fileSizeBuffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      fileSizeBuffer.putLong(fileInfo._size);
      fileSizeBuffer.flip();
      outputChannel.write(fileSizeBuffer);

      dataOffset += fileInfo._size;
    }
  }

  /**
   * Writes the file data section to the file.
   */
  private static void writeFileData(FileChannel outputChannel, Map<String, FileInfo> fileInfoMap)
      throws IOException {
    for (FileInfo fileInfo : fileInfoMap.values()) {
      try (FileChannel sourceChannel = FileChannel.open(fileInfo._file.toPath(), StandardOpenOption.READ)) {
        long transferred = 0;
        long fileSize = fileInfo._size;
        while (transferred < fileSize) {
          transferred += sourceChannel.transferTo(transferred, fileSize - transferred, outputChannel);
        }
        LOGGER.debug("Wrote file {} ({} bytes) to file", fileInfo._name, fileSize);
      } catch (IOException e) {
        throw new IOException("Failed to read file: " + fileInfo._file.getAbsolutePath(), e);
      }
    }
  }

  /**
   * Internal class to hold file information.
   */
  private static class FileInfo {
    final File _file;
    final String _name;
    final long _size;

    FileInfo(File file, String name, long size) {
      _file = file;
      _name = name;
      _size = size;
    }
  }
}
