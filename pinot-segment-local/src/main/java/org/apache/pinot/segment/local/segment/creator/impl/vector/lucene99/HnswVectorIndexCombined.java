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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneCombinedTextIndexConstants;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utility class to pack a Lucene HNSW index directory (and its optional docId mapping file) into
/// a single combined file using the {@link LuceneCombinedTextIndexConstants#MAGIC_NUMBER LUCENE_V2}
/// layout. Mirrors {@code LuceneTextIndexCombined} — reuses the same format constants to keep a
/// single on-disk format shared across text and HNSW vector indexes.
///
/// Layout (identical to the text-index LUCENE_V2 format):
/// ```
/// [Header]
///   Magic "LUCENE_V2"   9 bytes
///   Version             4 bytes (little-endian int)
///   Total buffer size   8 bytes (little-endian long)
///   File count          4 bytes (little-endian int)
///   Reserved            4 bytes
///
/// [File metadata, one entry per file]
///   Name length         2 bytes (little-endian short)
///   Name                variable
///   File offset         8 bytes (little-endian long)
///   File size           8 bytes (little-endian long)
///
/// [File data]
///   Raw bytes of each file concatenated in metadata order
/// ```
public final class HnswVectorIndexCombined {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVectorIndexCombined.class);

  private HnswVectorIndexCombined() {
  }

  /// Packs all files in {@code hnswIndexDir} (plus the optional docId mapping file) into a single
  /// combined file at {@code outputFilePath}.
  ///
  /// @param hnswIndexDir     the Lucene HNSW index directory to pack
  /// @param outputFilePath   destination path for the combined file
  /// @param segmentIndexDir  when non-null, the segment's top-level index directory; used to
  ///                         locate the docId mapping file for inclusion in the packed output
  /// @param column           column name; used to locate the docId mapping file when
  ///                         {@code segmentIndexDir} is provided
  /// @throws IOException if any file operations fail
  public static void combineHnswIndexFiles(File hnswIndexDir, String outputFilePath,
      @Nullable File segmentIndexDir, @Nullable String column)
      throws IOException {
    if (!hnswIndexDir.exists() || !hnswIndexDir.isDirectory()) {
      throw new IllegalArgumentException(
          "HNSW index directory does not exist or is not a directory: " + hnswIndexDir);
    }

    LOGGER.info("Combining HNSW index files from directory: {}", hnswIndexDir.getAbsolutePath());

    Map<String, FileInfo> fileInfoMap = collectFiles(hnswIndexDir, segmentIndexDir, column);
    int fileCount = fileInfoMap.size();

    if (fileCount == 0) {
      throw new IOException("No files found in HNSW index directory: " + hnswIndexDir);
    }

    long totalSize = calculateTotalBufferSize(fileInfoMap);
    if (totalSize > Integer.MAX_VALUE) {
      throw new IOException("Combined HNSW index size too large: " + totalSize + " bytes");
    }

    File outputFile = new File(outputFilePath);
    // TRUNCATE_EXISTING so a leftover (larger) file from a previously-crashed pack does not leave
    // stale trailing bytes past the new payload — that would inflate the file length and break the
    // size-based crash-recovery check in VectorIndexHandler that compares the combined file length
    // to the columns.psf typed-entry size.
    try (FileChannel outputChannel = FileChannel.open(outputFile.toPath(), StandardOpenOption.CREATE,
        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
      writeHeader(outputChannel, fileCount, (int) totalSize);
      long dataOffset = LuceneCombinedTextIndexConstants.getHeaderSize() + calculateMetadataSize(fileInfoMap);
      writeFileMetadata(outputChannel, fileInfoMap, dataOffset);
      writeFileData(outputChannel, fileInfoMap);
    }

    LOGGER.info("Combined {} HNSW index files into: {} ({} bytes)", fileCount, outputFilePath, totalSize);
  }

  /// Collects all regular files from {@code hnswIndexDir} and optionally the docId mapping file
  /// from the segment's flat directory. Uses a {@link TreeMap} for deterministic ordering.
  private static Map<String, FileInfo> collectFiles(File hnswIndexDir, @Nullable File segmentIndexDir,
      @Nullable String column)
      throws IOException {
    Map<String, FileInfo> fileInfoMap = new TreeMap<>();

    File[] files = hnswIndexDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          fileInfoMap.put(file.getName(), new FileInfo(file, file.getName(), file.length()));
        }
      }
    }

    // Include the docId mapping file when available. It lives beside the HNSW directory, not
    // inside it (same convention as text-index).
    if (segmentIndexDir != null && column != null) {
      File segmentDir = SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir);
      File mappingFile = new File(segmentDir,
          column + V1Constants.Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
      if (mappingFile.exists() && mappingFile.isFile()) {
        fileInfoMap.put(mappingFile.getName(), new FileInfo(mappingFile, mappingFile.getName(), mappingFile.length()));
        LOGGER.info("Including docId mapping file: {} ({} bytes)", mappingFile.getName(), mappingFile.length());
      }
    }

    return fileInfoMap;
  }

  /// Extracts files packed inside a combined HNSW file back into a Lucene directory.
  ///
  /// This is the inverse of {@link #combineHnswIndexFiles}. The docId mapping file (if present)
  /// is extracted into {@code targetDir} alongside the Lucene index files; the caller is
  /// responsible for moving it to its canonical location if needed.
  ///
  /// @param combinedFile source combined file (LUCENE_V2 layout)
  /// @param targetDir    destination directory; created if absent
  /// @throws IOException if any file operations fail
  public static void extractHnswIndexFiles(File combinedFile, File targetDir)
      throws IOException {
    if (!combinedFile.exists() || !combinedFile.isFile()) {
      throw new IllegalArgumentException("Combined file does not exist or is not a file: " + combinedFile);
    }
    FileUtils.forceMkdir(targetDir);

    try (FileChannel inputChannel = FileChannel.open(combinedFile.toPath(), StandardOpenOption.READ)) {
      // Parse header
      byte[] magicBytes = new byte[LuceneCombinedTextIndexConstants.MAGIC_NUMBER_LENGTH];
      readFully(inputChannel, ByteBuffer.wrap(magicBytes));
      String magic = new String(magicBytes);
      if (!LuceneCombinedTextIndexConstants.MAGIC_NUMBER.equals(magic)) {
        throw new IOException("Invalid magic number in combined HNSW file: " + magic);
      }

      ByteBuffer intBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      readFully(inputChannel, intBuf);
      intBuf.flip();
      int version = intBuf.getInt();
      if (version != LuceneCombinedTextIndexConstants.VERSION) {
        throw new IOException("Unsupported version in combined HNSW file: " + version);
      }

      // Skip total size (8 bytes) and file count (4 bytes) header fields
      ByteBuffer longBuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      readFully(inputChannel, longBuf); // totalSize
      longBuf.flip();
      // (unused, but we advance the channel position past it)

      intBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      readFully(inputChannel, intBuf);
      intBuf.flip();
      int fileCount = intBuf.getInt();

      // Skip reserved field
      intBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      readFully(inputChannel, intBuf);

      // Parse file metadata
      String[] fileNames = new String[fileCount];
      long[] fileOffsets = new long[fileCount];
      long[] fileSizes = new long[fileCount];
      for (int i = 0; i < fileCount; i++) {
        ByteBuffer shortBuf = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        readFully(inputChannel, shortBuf);
        shortBuf.flip();
        short nameLength = shortBuf.getShort();

        byte[] nameBytes = new byte[nameLength];
        readFully(inputChannel, ByteBuffer.wrap(nameBytes));
        fileNames[i] = new String(nameBytes);

        longBuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        readFully(inputChannel, longBuf);
        longBuf.flip();
        fileOffsets[i] = longBuf.getLong();

        longBuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        readFully(inputChannel, longBuf);
        longBuf.flip();
        fileSizes[i] = longBuf.getLong();
      }

      // Extract each file by seeking to its offset and copying bytes
      for (int i = 0; i < fileCount; i++) {
        File outFile = new File(targetDir, fileNames[i]);
        long fileSize = fileSizes[i];
        try (FileChannel outChannel = FileChannel.open(outFile.toPath(), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE)) {
          long remaining = fileSize;
          long srcOffset = fileOffsets[i];
          while (remaining > 0) {
            long transferred = inputChannel.transferTo(srcOffset, remaining, outChannel);
            if (transferred <= 0) {
              // transferTo returns 0 when srcOffset is at/after EOF (corrupt offsets/sizes in the
              // header). Bail out instead of spinning forever on a malformed combined file.
              throw new IOException("Truncated or corrupt combined HNSW file: expected " + fileSize
                  + " bytes for " + fileNames[i] + " at offset " + fileOffsets[i] + " but " + remaining
                  + " bytes remain unreadable in " + combinedFile);
            }
            srcOffset += transferred;
            remaining -= transferred;
          }
        }
        LOGGER.debug("Extracted {} ({} bytes) from combined HNSW file", fileNames[i], fileSize);
      }
    }
    LOGGER.info("Extracted {} files from combined HNSW file: {} into: {}", targetDir.listFiles() == null ? 0
        : targetDir.listFiles().length, combinedFile.getAbsolutePath(), targetDir.getAbsolutePath());
  }

  // ---- header / metadata write helpers ----

  private static void writeHeader(FileChannel outputChannel, int fileCount, int totalSize)
      throws IOException {
    outputChannel.write(ByteBuffer.wrap(LuceneCombinedTextIndexConstants.MAGIC_NUMBER.getBytes()));

    ByteBuffer versionBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    versionBuf.putInt(LuceneCombinedTextIndexConstants.VERSION);
    versionBuf.flip();
    outputChannel.write(versionBuf);

    ByteBuffer totalSizeBuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    totalSizeBuf.putLong(totalSize);
    totalSizeBuf.flip();
    outputChannel.write(totalSizeBuf);

    ByteBuffer fileCountBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    fileCountBuf.putInt(fileCount);
    fileCountBuf.flip();
    outputChannel.write(fileCountBuf);

    // Reserved field — zero for now
    ByteBuffer reservedBuf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    reservedBuf.putInt(0);
    reservedBuf.flip();
    outputChannel.write(reservedBuf);
  }

  private static void writeFileMetadata(FileChannel outputChannel, Map<String, FileInfo> fileInfoMap, long dataOffset)
      throws IOException {
    for (FileInfo fileInfo : fileInfoMap.values()) {
      ByteBuffer nameLengthBuf = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      nameLengthBuf.putShort((short) fileInfo._name.length());
      nameLengthBuf.flip();
      outputChannel.write(nameLengthBuf);

      outputChannel.write(ByteBuffer.wrap(fileInfo._name.getBytes()));

      ByteBuffer fileOffsetBuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      fileOffsetBuf.putLong(dataOffset);
      fileOffsetBuf.flip();
      outputChannel.write(fileOffsetBuf);

      ByteBuffer fileSizeBuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      fileSizeBuf.putLong(fileInfo._size);
      fileSizeBuf.flip();
      outputChannel.write(fileSizeBuf);

      dataOffset += fileInfo._size;
    }
  }

  private static void writeFileData(FileChannel outputChannel, Map<String, FileInfo> fileInfoMap)
      throws IOException {
    for (FileInfo fileInfo : fileInfoMap.values()) {
      try (FileChannel sourceChannel = FileChannel.open(fileInfo._file.toPath(), StandardOpenOption.READ)) {
        long transferred = 0;
        long fileSize = fileInfo._size;
        while (transferred < fileSize) {
          transferred += sourceChannel.transferTo(transferred, fileSize - transferred, outputChannel);
        }
        LOGGER.debug("Wrote {} ({} bytes) into combined HNSW file", fileInfo._name, fileSize);
      } catch (IOException e) {
        throw new IOException("Failed to read file: " + fileInfo._file.getAbsolutePath(), e);
      }
    }
  }

  private static long calculateTotalBufferSize(Map<String, FileInfo> fileInfoMap) {
    long total = LuceneCombinedTextIndexConstants.getHeaderSize();
    total += calculateMetadataSize(fileInfoMap);
    for (FileInfo fi : fileInfoMap.values()) {
      total += fi._size;
    }
    return total;
  }

  private static long calculateMetadataSize(Map<String, FileInfo> fileInfoMap) {
    long size = 0;
    for (FileInfo fi : fileInfoMap.values()) {
      size += LuceneCombinedTextIndexConstants.getFileMetadataEntrySizeWithFilename(fi._name);
    }
    return size;
  }

  /// Reads a buffer fully from the channel, throwing if EOF is reached prematurely.
  private static void readFully(FileChannel channel, ByteBuffer buf)
      throws IOException {
    while (buf.hasRemaining()) {
      int n = channel.read(buf);
      if (n < 0) {
        throw new IOException("Unexpected end-of-file in combined HNSW index");
      }
    }
  }

  private static final class FileInfo {
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
