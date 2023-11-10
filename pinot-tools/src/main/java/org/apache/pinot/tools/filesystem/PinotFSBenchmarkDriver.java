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
package org.apache.pinot.tools.filesystem;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotFSBenchmarkDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSBenchmarkDriver.class);

  private static final int DEFAULT_NUM_SEGMENTS_FOR_LIST_TEST = 1000;
  private static final int DEFAULT_DATA_SIZE_IN_MB_FOR_COPY_TEST = 1024; // 1GB
  private static final int DEFAULT_NUM_OPS = 5; // 5

  private String _mode;
  private PinotFS _pinotFS;
  private URI _baseDirectoryUri;
  private File _localTempDir;
  private int _numSegmentsForListFilesTest;
  private int _numOps;
  private int _dataSizeInMBsForCopyTest;

  public PinotFSBenchmarkDriver(String mode, String configFilePath, String baseDirectoryUri, String localTempDir,
      Integer numSegmentsForListFilesTest, Integer dataSizeInMBsForCopyTest, Integer numOps)
      throws ConfigurationException {
    PropertiesConfiguration configuration =
        CommonsConfigurationUtils.loadFromFile(new File(configFilePath));
    PinotFSFactory.init(new PinotConfiguration(configuration));
    _mode = mode;
    _baseDirectoryUri = URI.create(baseDirectoryUri);
    _pinotFS = PinotFSFactory.create(_baseDirectoryUri.getScheme());
    _localTempDir =
        (localTempDir != null) ? new File(localTempDir) : new File(FileUtils.getTempDirectory(), "benchmark");
    _numSegmentsForListFilesTest =
        (numSegmentsForListFilesTest != null) ? numSegmentsForListFilesTest : DEFAULT_NUM_SEGMENTS_FOR_LIST_TEST;
    _numOps = (numOps != null) ? numOps : DEFAULT_NUM_OPS;
    _dataSizeInMBsForCopyTest =
        (dataSizeInMBsForCopyTest != null) ? dataSizeInMBsForCopyTest : DEFAULT_DATA_SIZE_IN_MB_FOR_COPY_TEST;
    LOGGER.info("PinotFS has been initialized sucessfully. (mode = {}, pinotFSClass = {}, configFile = {}, "
            + "baseDirectoryUri = {}, localTempDir = {}, numSegmentsForListFilesTest = {}, "
            + "dataSizeInMBsForCopyTest = {}, numOps = {})", _mode, _pinotFS.getClass().getSimpleName(), configFilePath,
        baseDirectoryUri, _localTempDir, _numSegmentsForListFilesTest, _dataSizeInMBsForCopyTest, _numOps);
  }

  public void run()
      throws Exception {
    prepareBenchmark();

    switch (_mode.toUpperCase()) {
      case "ALL":
        testListFilesInMultipleDirectories();
        testListFiles();
        testCopies();
        break;
      case "LISTFILES":
        testListFiles();
        break;
      case "COPY":
        testCopies();
        break;
      default:
        throw new RuntimeException("Not Supported Mode: " + _mode);
    }
    cleanUpBenchmark();
  }

  private void prepareBenchmark()
      throws IOException {
    // Clean up base directory
    if (_pinotFS.exists(_baseDirectoryUri)) {
      _pinotFS.delete(_baseDirectoryUri, true);
    }

    if (_localTempDir.exists()) {
      _localTempDir.delete();
    }

    // Set up the base directory
    _pinotFS.mkdir(_baseDirectoryUri);
    _localTempDir.mkdir();
  }

  private void cleanUpBenchmark()
      throws IOException {
    _pinotFS.delete(_baseDirectoryUri, true);
    FileUtils.deleteQuietly(_localTempDir);
    LOGGER.info("Working directories have been cleaned up successfully. (baseDirectoryUri={}, localTempDir={})",
        _baseDirectoryUri, _localTempDir);
  }

  private void testListFilesInMultipleDirectories()
      throws Exception {
    LOGGER.info("========= List Files in Multiple Directories ==========");
    long prepareTime = System.currentTimeMillis();
    URI listTestUri = combinePath(_baseDirectoryUri, "listTestMultipleFile");
    _pinotFS.mkdir(listTestUri);
    LOGGER.info("Created {} for list test...", listTestUri);

    int numSegments = 1;
    for (int i = 0; i < 5; i++) {
      String directoryPath = "directory_" + i;
      File tmpDirectory = new File(_localTempDir.getPath(), directoryPath);
      URI directoryUri = combinePath(listTestUri, directoryPath);
      tmpDirectory.mkdir();

      for (int j = 0; j < numSegments; j++) {
        String relativePath = "segment_" + j;
        File tmpFile = new File(tmpDirectory, relativePath);
        tmpFile.createNewFile();
        _pinotFS.copyFromLocalFile(tmpFile, combinePath(directoryUri, relativePath));
      }
      LOGGER.info("Took {} ms to create {} segments for directory_{}", System.currentTimeMillis() - prepareTime,
          numSegments, i);
      numSegments *= 10;
    }

    // reset numSegments
    numSegments = 1;
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < _numOps; j++) {
        URI directoryUri = combinePath(listTestUri, "directory_" + i);
        long listFilesStart = System.currentTimeMillis();
        String[] lists = _pinotFS.listFiles(directoryUri, true);
        LOGGER.info("{}: took {} ms to listFiles. directory_{} ({} segments)", j,
            System.currentTimeMillis() - listFilesStart, i, lists.length);
        Preconditions.checkState(lists.length == numSegments);
      }
      numSegments *= 10;
    }
  }

  private void testListFiles()
      throws Exception {
    LOGGER.info("========= List Files ==========");
    long testStartTime = System.currentTimeMillis();
    URI listTestUri = combinePath(_baseDirectoryUri, "listTest");
    _pinotFS.mkdir(listTestUri);
    LOGGER.info("Created {} for list test...", listTestUri);

    for (int i = 0; i < _numSegmentsForListFilesTest; i++) {
      String relativePath = "segment_" + i;
      File tmpFile = new File(_localTempDir.getPath(), relativePath);
      tmpFile.createNewFile();
      _pinotFS.copyFromLocalFile(tmpFile, combinePath(listTestUri, relativePath));
    }
    LOGGER.info("Took {} ms to create {} segments.", System.currentTimeMillis() - testStartTime,
        _numSegmentsForListFilesTest);

    for (int i = 0; i < _numOps; i++) {
      long listFilesStart = System.currentTimeMillis();
      String[] lists = _pinotFS.listFiles(listTestUri, true);
      LOGGER.info("{}: took {} ms to listFiles.", i, System.currentTimeMillis() - listFilesStart);
      Preconditions.checkState(lists.length == _numSegmentsForListFilesTest);
    }
  }

  private void testCopies()
      throws Exception {
    LOGGER.info("\n========= Uploads and Downloads ==========");
    URI copyTestUri = combinePath(_baseDirectoryUri, "copyFiles");
    _pinotFS.mkdir(copyTestUri);
    LOGGER.info("Created {} for copy test...", copyTestUri);

    long fileSizeInBytes = _dataSizeInMBsForCopyTest * 1024 * 1024;
    File largeTmpFile = createFileWithSize("largeFile", fileSizeInBytes);
    for (int i = 0; i < _numOps; i++) {
      URI largeFileDstUri = combinePath(copyTestUri, largeTmpFile.getName() + "_" + i);
      long copyStart = System.currentTimeMillis();
      _pinotFS.copyFromLocalFile(largeTmpFile, largeFileDstUri);
      LOGGER.info("{}: took {} ms to copyFromLocal, fileSize: {} MB.", i, System.currentTimeMillis() - copyStart,
          _dataSizeInMBsForCopyTest);
    }

    for (int i = 0; i < _numOps; i++) {
      URI largeFileSrcUri = combinePath(copyTestUri, largeTmpFile.getName() + "_" + i);
      File localTmpLargeFile = new File(_localTempDir, "largeFile_" + i);
      long copyStart = System.currentTimeMillis();
      _pinotFS.copyToLocalFile(largeFileSrcUri, localTmpLargeFile);
      LOGGER.info("{}: took {} ms to copyToLocal, fileSize: {} MB.", i, System.currentTimeMillis() - copyStart,
          _dataSizeInMBsForCopyTest);
    }

    for (int i = 0; i < _numOps; i++) {
      URI largeFileSrcUri = combinePath(copyTestUri, largeTmpFile.getName() + "_" + i);
      URI largeFileDstUri = combinePath(copyTestUri, largeTmpFile.getName() + "_copy_" + i);

      long copyStart = System.currentTimeMillis();
      _pinotFS.copy(largeFileSrcUri, largeFileDstUri);
      LOGGER.info("{}: took {} ms to copy, fileSize: {} MB.", i, System.currentTimeMillis() - copyStart,
          _dataSizeInMBsForCopyTest);
    }

    for (int i = 0; i < _numOps; i++) {
      URI largeFileSrcUri = combinePath(copyTestUri, largeTmpFile.getName() + "_copy_" + i);
      URI largeFileDstUri = combinePath(copyTestUri, largeTmpFile.getName() + "_rename_" + i);

      long renameStart = System.currentTimeMillis();
      _pinotFS.move(largeFileSrcUri, largeFileDstUri, true);
      LOGGER.info("{}: took {} ms to rename, fileSize: {} MB.", i, System.currentTimeMillis() - renameStart,
          _dataSizeInMBsForCopyTest);
    }

    for (int i = 0; i < _numOps; i++) {
      URI largeFileDstUri = combinePath(copyTestUri, largeTmpFile.getName() + "_" + i);
      long deleteStart = System.currentTimeMillis();
      _pinotFS.delete(largeFileDstUri, true);
      LOGGER.info("{}: took {} ms to delete, fileSize: {} MB.", i, System.currentTimeMillis() - deleteStart,
          _dataSizeInMBsForCopyTest);
    }
  }

  private File createFileWithSize(String fileName, long sizeInBytes)
      throws IOException {
    File tmpLargeFile = new File(_localTempDir, fileName);
    tmpLargeFile.createNewFile();
    RandomAccessFile raf = new RandomAccessFile(tmpLargeFile, "rw");
    raf.setLength(sizeInBytes);
    raf.close();
    return tmpLargeFile;
  }

  private URI combinePath(URI baseUri, String path)
      throws URISyntaxException {
    return new URI(baseUri.getScheme(), baseUri.getHost(), baseUri.getPath() + File.separator + path, null);
  }
}
