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
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Taken from http://www.thoughtspark.org/node/53
 *
 */
public class TarGzCompressionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TarGzCompressionUtils.class);
  public static final String TAR_GZ_FILE_EXTENSION = ".tar.gz";

  /**
   * Creates a tar.gz file at the specified path with the contents of the
   * specified directory.
   *
   * @param directoryPath
   *          The path to the directory to create an archive of
   * @param tarGzPath
   *          The path to the archive to create. The file may not exist but
   *          it's parent must exist and the parent must be a directory
   * @return tarGzPath
   * @throws IOException
   *           If anything goes wrong
   */
  public static String createTarGzOfDirectory(String directoryPath, String tarGzPath) throws IOException {
    return createTarGzOfDirectory(directoryPath, tarGzPath, "");
  }

  public static String createTarGzOfDirectory(String directoryPath, String tarGzPath, String entryPrefix)
      throws IOException {
    if (!tarGzPath.endsWith(TAR_GZ_FILE_EXTENSION)) {
      tarGzPath = tarGzPath + TAR_GZ_FILE_EXTENSION;
    }
    try (
        FileOutputStream fOut = new FileOutputStream(new File(tarGzPath));
        BufferedOutputStream bOut = new BufferedOutputStream(fOut);
        GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(bOut);
        TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)
    ) {
      tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
      addFileToTarGz(tOut, directoryPath, entryPrefix);
    } catch (IOException e) {
      LOGGER.error("Failed to create tar.gz file for {} at path: {}", directoryPath, tarGzPath, e);
      Utils.rethrowException(e);
    }
    return tarGzPath;
  }

  public static String createTarGzOfDirectory(String directoryPath) throws IOException {
    String tarGzPath = directoryPath.substring(0);
    while (tarGzPath.endsWith("/")) {
      tarGzPath = tarGzPath.substring(0, tarGzPath.length() - 1);
    }
    tarGzPath = tarGzPath + TAR_GZ_FILE_EXTENSION;
    return createTarGzOfDirectory(directoryPath, tarGzPath);
  }

  /**
   * Creates a tar entry for the path specified with a name built from the base
   * passed in and the file/directory name. If the path is a directory, a
   * recursive call is made such that the full directory is added to the tar.
   *
   * @param tOut
   *          The tar file's output stream
   * @param path
   *          The filesystem path of the file/directory being added
   * @param base
   *          The base prefix to for the name of the tar file entry
   *
   * @throws IOException
   *           If anything goes wrong
   */
  private static void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base) throws IOException {
    File f = new File(path);
    String entryName = base + f.getName();
    TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);

    tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tOut.putArchiveEntry(tarEntry);

    if (f.isFile()) {
      IOUtils.copy(new FileInputStream(f), tOut);

      tOut.closeArchiveEntry();
    } else {
      tOut.closeArchiveEntry();

      File[] children = f.listFiles();

      if (children != null) {
        for (File child : children) {
          addFileToTarGz(tOut, child.getAbsolutePath(), entryName + "/");
        }
      }
    }
  }

  /** Untar an input file into an output file.

   * The output file is created in the output folder, having the same name
   * as the input file, minus the '.tar' extension.
   *
   * @param inputFile     the input .tar file
   * @param outputDir     the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   *
   * @return The {@link List} of {@link File}s with the untared content.
   * @throws ArchiveException
   */
  public static List<File> unTar(final File inputFile, final File outputDir)
      throws IOException, ArchiveException {

    String outputDirectoryPath = outputDir.getCanonicalPath();
    LOGGER.debug("Untaring {} to dir {}.", inputFile.getAbsolutePath(), outputDirectoryPath);
    TarArchiveInputStream debInputStream = null;
    InputStream is = null;
    final List<File> untaredFiles = new LinkedList<File>();
    try {
      is = new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(inputFile)));
      debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        final File outputFile = new File(outputDir, entry.getName());
        // Check whether the untarred file will be put outside of the target output directory.
        if (!outputFile.getCanonicalPath().startsWith(outputDirectoryPath)) {
          throw new IOException("Tar file must not be untarred outside of the target output directory!");
        }
        if (entry.isDirectory()) {
          LOGGER.debug(String.format("Attempting to write output directory %s.", outputFile.getAbsolutePath()));
          if (!outputFile.exists()) {
            LOGGER.debug(String.format("Attempting to create output directory %s.", outputFile.getAbsolutePath()));
            if (!outputFile.mkdirs()) {
              throw new IllegalStateException(
                  String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
            }
          } else {
            LOGGER.error("The directory already there. Deleting - " + outputFile.getAbsolutePath());
            FileUtils.deleteDirectory(outputFile);
          }
        } else {
          LOGGER.debug(String.format("Creating output file %s.", outputFile.getAbsolutePath()));
          File directory = outputFile.getParentFile();
          if (!directory.exists()) {
            directory.mkdirs();
          }
          OutputStream outputFileStream = null;
          try {
            outputFileStream = new FileOutputStream(outputFile);
            IOUtils.copy(debInputStream, outputFileStream);
          } finally {
            IOUtils.closeQuietly(outputFileStream);
          }
        }
        untaredFiles.add(outputFile);
      }
    } finally {
      IOUtils.closeQuietly(debInputStream);
      IOUtils.closeQuietly(is);
    }
    return untaredFiles;
  }

  public static InputStream unTarOneFile(InputStream tarGzInputStream, final String filename)
      throws FileNotFoundException, IOException, ArchiveException {
    TarArchiveInputStream debInputStream = null;
    InputStream is = null;
    try {
      is = new GzipCompressorInputStream(tarGzInputStream);
      debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        if (entry.getName().contains(filename)) {
          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          IOUtils.copy(debInputStream, byteArrayOutputStream);
          return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        }
      }
    } finally {
      IOUtils.closeQuietly(debInputStream);
      IOUtils.closeQuietly(is);
    }
    return null;
  }
}
