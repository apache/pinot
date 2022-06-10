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
package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;


/**
 * Utility class to compress/de-compress tar.gz files.
 */
public class TarGzCompressionUtils {
  public static final long NO_DISK_WRITE_RATE_LIMIT = -1;
  /* Don't limit write rate to disk. The OS will buffer multiple writes and can write up to several GBs
   * at a time, which saturates disk bandwidth.
   */
  public static final long SYNC_DISK_WRITE_WITH_UPSTREAM_RATE = 0;
  /* Match the upstream rate, but will do a file sync for each write of DEFAULT_BUFFER_SIZE
   * to flush the buffer to disk. This avoids saturating disk I/O bandwidth.
   */
  private static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
  /* 4MB is large enough, page aligned, and multiple of SSD block size for efficient write:
   * Common page sizes are 2K, 4K, 8K, or 16K, with 128 to 256 pages per block.
   * Block size therefore typically varies between 256KB and 4MB.
   * https://codecapsule.com/2014/02/12
   * /coding-for-ssds-part-6-a-summary-what-every-programmer-should-know-about-solid-state-drives/
   *
   * It is also sufficient for HDDs
   */


  private TarGzCompressionUtils() {
  }

  public static final String TAR_GZ_FILE_EXTENSION = ".tar.gz";
  private static final char ENTRY_NAME_SEPARATOR = '/';

  /**
   * Creates a tar.gz file from the input file/directory to the output file. The output file must have ".tar.gz" as the
   * file extension.
   */
  public static void createTarGzFile(File inputFile, File outputFile)
      throws IOException {
    createTarGzFile(new File[]{inputFile}, outputFile);
  }

  /**
   * Creates a tar.gz file from a list of input file/directories to the output file. The output file must have
   * ".tar.gz" as the file extension.
   */
  public static void createTarGzFile(File[] inputFiles, File outputFile)
      throws IOException {
    Preconditions.checkArgument(outputFile.getName().endsWith(TAR_GZ_FILE_EXTENSION),
        "Output file: %s does not have '.tar.gz' file extension", outputFile);
    try (OutputStream fileOut = Files.newOutputStream(outputFile.toPath());
        BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOut);
        OutputStream gzipOut = new GzipCompressorOutputStream(bufferedOut);
        TarArchiveOutputStream tarGzOut = new TarArchiveOutputStream(gzipOut)) {
      tarGzOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
      tarGzOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

      for (File inputFile : inputFiles) {
        addFileToTarGz(tarGzOut, inputFile, "");
      }
    }
  }

  /**
   * Helper method to write a file into the tar.gz file output stream. The base entry name is the relative path of the
   * file to the root directory.
   */
  private static void addFileToTarGz(ArchiveOutputStream tarGzOut, File file, String baseEntryName)
      throws IOException {
    String entryName = baseEntryName + file.getName();
    TarArchiveEntry entry = new TarArchiveEntry(file, entryName);
    tarGzOut.putArchiveEntry(entry);
    if (file.isFile()) {
      try (InputStream in = Files.newInputStream(file.toPath())) {
        IOUtils.copy(in, tarGzOut);
      }
      tarGzOut.closeArchiveEntry();
    } else {
      tarGzOut.closeArchiveEntry();

      File[] children = file.listFiles();
      assert children != null;
      String baseEntryNameForChildren = entryName + ENTRY_NAME_SEPARATOR;
      for (File child : children) {
        addFileToTarGz(tarGzOut, child, baseEntryNameForChildren);
      }
    }
  }

  /**
   * Un-tars a tar.gz file into a directory, returns all the untarred files/directories.
   * <p>For security reason, the untarred files must reside in the output directory.
   */
  public static List<File> untar(File inputFile, File outputDir)
      throws IOException {
    try (InputStream fileIn = Files.newInputStream(inputFile.toPath())) {
      return untar(fileIn, outputDir);
    }
  }

  /**
   * Un-tars an inputstream of a tar.gz file into a directory, returns all the untarred files/directories.
   * <p>For security reason, the untarred files must reside in the output directory.
   */
  public static List<File> untar(InputStream inputStream, File outputDir)
      throws IOException {
    return untarWithRateLimiter(inputStream, outputDir, NO_DISK_WRITE_RATE_LIMIT);
  }

  /**
   * Un-tars an inputstream of a tar.gz file into a directory, returns all the untarred files/directories.
   * RateLimit limits the untar rate
   * <p>For security reason, the untarred files must reside in the output directory.
   */
  public static List<File> untarWithRateLimiter(InputStream inputStream, File outputDir, long maxStreamRateInByte)
      throws IOException {
    String outputDirCanonicalPath = outputDir.getCanonicalPath();
    // Prevent partial path traversal
    if (!outputDirCanonicalPath.endsWith(File.separator)) {
      outputDirCanonicalPath += File.separator;
    }
    List<File> untarredFiles = new ArrayList<>();
    try (InputStream bufferedIn = new BufferedInputStream(inputStream);
        InputStream gzipIn = new GzipCompressorInputStream(bufferedIn);
        ArchiveInputStream tarGzIn = new TarArchiveInputStream(gzipIn)) {
      ArchiveEntry entry;
      while ((entry = tarGzIn.getNextEntry()) != null) {
        String entryName = entry.getName();
        String[] parts = StringUtils.split(entryName, ENTRY_NAME_SEPARATOR);
        File outputFile = outputDir;
        for (String part : parts) {
          outputFile = new File(outputFile, part);
        }
        if (entry.isDirectory()) {
          if (!outputFile.getCanonicalPath().startsWith(outputDirCanonicalPath)) {
            throw new IOException(String
                .format("Trying to create directory: %s outside of the output directory: %s", outputFile, outputDir));
          }
          if (!outputFile.isDirectory() && !outputFile.mkdirs()) {
            throw new IOException(String.format("Failed to create directory: %s", outputFile));
          }
        } else {
          File parentFile = outputFile.getParentFile();
          String parentFileCanonicalPath = parentFile.getCanonicalPath();

          // Ensure parentFile's canonical path is separator terminated, since outputDirCanonicalPath is.
          if (!parentFileCanonicalPath.endsWith(File.separator)) {
            parentFileCanonicalPath += File.separator;
          }
          if (!parentFileCanonicalPath.startsWith(outputDirCanonicalPath)) {
            throw new IOException(String
                .format("Trying to create directory: %s outside of the output directory: %s", parentFile, outputDir));
          }
          if (!parentFile.isDirectory() && !parentFile.mkdirs()) {
            throw new IOException(String.format("Failed to create directory: %s", parentFile));
          }
          try (FileOutputStream out = new FileOutputStream(outputFile.toPath().toString())) {
            if (maxStreamRateInByte != NO_DISK_WRITE_RATE_LIMIT) {
              copyWithRateLimiter(tarGzIn, out, maxStreamRateInByte);
            } else {
              IOUtils.copy(tarGzIn, out);
            }
          }
        }
        untarredFiles.add(outputFile);
      }
    }
    return untarredFiles;
  }

  /**
   * Un-tars one single file with the given file name from a tar.gz file.
   */
  public static void untarOneFile(File inputFile, String fileName, File outputFile)
      throws IOException {
    try (InputStream fileIn = Files.newInputStream(inputFile.toPath());
        InputStream bufferedIn = new BufferedInputStream(fileIn);
        InputStream gzipIn = new GzipCompressorInputStream(bufferedIn);
        ArchiveInputStream tarGzIn = new TarArchiveInputStream(gzipIn)) {
      ArchiveEntry entry;
      while ((entry = tarGzIn.getNextEntry()) != null) {
        if (!entry.isDirectory()) {
          String entryName = entry.getName();
          String[] parts = StringUtils.split(entryName, ENTRY_NAME_SEPARATOR);
          if (parts.length > 0 && parts[parts.length - 1].equals(fileName)) {
            try (OutputStream out = Files.newOutputStream(outputFile.toPath())) {
              IOUtils.copy(tarGzIn, out);
            }
            return;
          }
        }
      }
      throw new IOException(String.format("Failed to find file: %s in: %s", fileName, inputFile));
    }
  }

  public static long copyWithRateLimiter(InputStream inputStream, FileOutputStream outputStream,
      long maxStreamRateInByte)
      throws IOException {
    Preconditions.checkState(inputStream != null, "inputStream is null");
    Preconditions.checkState(outputStream != null, "outputStream is null");
    FileDescriptor fd = outputStream.getFD();
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    long count;
    int n;

    if (maxStreamRateInByte == SYNC_DISK_WRITE_WITH_UPSTREAM_RATE) {
      for (count = 0L; -1 != (n = inputStream.read(buffer)); count += (long) n) {
        outputStream.write(buffer, 0, n);
        fd.sync(); // flush the buffer timely to the disk so that the disk bandwidth wouldn't get saturated
      }
    } else {
      RateLimiter rateLimiter = RateLimiter.create(maxStreamRateInByte);
      for (count = 0L; -1 != (n = inputStream.read(buffer)); count += (long) n) {
        rateLimiter.acquire(n);
        outputStream.write(buffer, 0, n);
        fd.sync();
      }
    }
    return count;
  }
}
