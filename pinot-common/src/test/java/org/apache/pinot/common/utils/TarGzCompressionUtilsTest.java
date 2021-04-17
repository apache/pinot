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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class TarGzCompressionUtilsTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "TarGzCompressionUtilsTest");
  private static final File DATA_DIR = new File(TEMP_DIR, "dataDir");
  private static final File TAR_DIR = new File(TEMP_DIR, "tarDir");
  private static final File UNTAR_DIR = new File(TEMP_DIR, "untarDir");

  @BeforeMethod
  public void setUp() throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(DATA_DIR);
    FileUtils.forceMkdir(TAR_DIR);
    FileUtils.forceMkdir(UNTAR_DIR);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testFile() throws IOException {
    String fileName = "data";
    String fileContent = "fileContent";
    File dataFile = new File(DATA_DIR, fileName);
    FileUtils.write(dataFile, fileContent);

    File tarGzFile = new File(TAR_DIR, fileName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(dataFile, tarGzFile);

    List<File> untarredFiles = TarGzCompressionUtils.untar(tarGzFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 1);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, fileName));
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarGzCompressionUtils.untarOneFile(tarGzFile, fileName, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent);
  }

  @Test
  public void testDirectory() throws IOException {
    String dirName = "dir";
    File dir = new File(DATA_DIR, dirName);
    String fileName1 = "data1";
    String fileContent1 = "fileContent1";
    String fileName2 = "data2";
    String fileContent2 = "fileContent2";
    FileUtils.write(new File(dir, fileName1), fileContent1);
    FileUtils.write(new File(dir, fileName2), fileContent2);

    File tarGzFile = new File(TAR_DIR, dirName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(dir, tarGzFile);

    List<File> untarredFiles = TarGzCompressionUtils.untar(tarGzFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 3);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    assertEquals(FileUtils.readFileToString(new File(untarredFile, fileName1)), fileContent1);
    assertEquals(FileUtils.readFileToString(new File(untarredFile, fileName2)), fileContent2);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarGzCompressionUtils.untarOneFile(tarGzFile, fileName1, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent1);
    TarGzCompressionUtils.untarOneFile(tarGzFile, fileName2, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent2);
    try {
      TarGzCompressionUtils.untarOneFile(tarGzFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testSubDirectories() throws IOException {
    String dirName = "dir";
    File dir = new File(DATA_DIR, dirName);
    String subDirName1 = "subDir1";
    File subDir1 = new File(dir, subDirName1);
    String subDirName2 = "subDir2";
    File subDir2 = new File(dir, subDirName2);
    String fileName1 = "data1";
    String fileContent1 = "fileContent1";
    String fileName2 = "data2";
    String fileContent2 = "fileContent2";
    FileUtils.write(new File(subDir1, fileName1), fileContent1);
    FileUtils.write(new File(subDir2, fileName2), fileContent2);

    File tarGzFile = new File(TAR_DIR, dirName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(dir, tarGzFile);

    List<File> untarredFiles = TarGzCompressionUtils.untar(tarGzFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 5);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    assertEquals(FileUtils.readFileToString(new File(new File(untarredFile, subDirName1), fileName1)), fileContent1);
    assertEquals(FileUtils.readFileToString(new File(new File(untarredFile, subDirName2), fileName2)), fileContent2);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarGzCompressionUtils.untarOneFile(tarGzFile, fileName1, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent1);
    TarGzCompressionUtils.untarOneFile(tarGzFile, fileName2, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent2);
    try {
      TarGzCompressionUtils.untarOneFile(tarGzFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
    try {
      TarGzCompressionUtils.untarOneFile(tarGzFile, subDirName1, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
    try {
      TarGzCompressionUtils.untarOneFile(tarGzFile, subDirName2, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testEmptyDirectory() throws IOException {
    String dirName = "dir";
    File dir = new File(DATA_DIR, dirName);
    FileUtils.forceMkdir(dir);

    File tarGzFile = new File(TAR_DIR, dirName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(dir, tarGzFile);

    List<File> untarredFiles = TarGzCompressionUtils.untar(tarGzFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 1);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 0);

    untarredFile = new File(UNTAR_DIR, "untarred");
    try {
      TarGzCompressionUtils.untarOneFile(tarGzFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testBadFilePath() throws IOException {
    String fileName = "data";
    String fileContent = "fileContent";
    File dataFile = new File(DATA_DIR, fileName);
    FileUtils.write(dataFile, fileContent);

    File badTarGzFile = new File(TAR_DIR, "bad" + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    try (OutputStream fileOut = Files.newOutputStream(badTarGzFile.toPath());
        OutputStream gzipOut = new GzipCompressorOutputStream(fileOut);
        TarArchiveOutputStream tarGzOut = new TarArchiveOutputStream(gzipOut)) {
      tarGzOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
      TarArchiveEntry entry = new TarArchiveEntry(dataFile, "../bad/path/data");
      tarGzOut.putArchiveEntry(entry);
      try (InputStream in = Files.newInputStream(dataFile.toPath())) {
        IOUtils.copy(in, tarGzOut);
      }
      tarGzOut.closeArchiveEntry();
    }

    try {
      TarGzCompressionUtils.untar(badTarGzFile, UNTAR_DIR);
      fail();
    } catch (IOException e) {
      // Expected
    }

    // Allow untar one file to the given destination
    File untarredFile = new File(UNTAR_DIR, "untarred");
    TarGzCompressionUtils.untarOneFile(badTarGzFile, fileName, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent);
  }
}
