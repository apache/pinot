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
import java.util.Map;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class TarCompressionUtilsTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), TarCompressionUtilsTest.class.getName());
  private static final File DATA_DIR = new File(TEMP_DIR, "dataDir");
  private static final File TAR_DIR = new File(TEMP_DIR, "tarDir");
  private static final File UNTAR_DIR = new File(TEMP_DIR, "untarDir");
  private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY = CompressorStreamFactory.getSingleton();

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(DATA_DIR);
    FileUtils.forceMkdir(TAR_DIR);
    FileUtils.forceMkdir(UNTAR_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testFile()
      throws IOException, CompressorException {
    for (String compressedTarFileExtension : TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.keySet()) {
      testFile(compressedTarFileExtension);
    }
  }

  public void testFile(String compressedTarFileExtension)
      throws IOException, CompressorException {
    String fileName = "data";
    String fileContent = "fileContent";
    File dataFile = new File(DATA_DIR, fileName);
    FileUtils.write(dataFile, fileContent);

    File compressedTarFile = new File(TAR_DIR, fileName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dataFile, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 1);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, fileName));
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent);
  }

  @Test
  public void testDirectories()
      throws IOException, CompressorException {
    for (String compressedTarFileExtension : TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.keySet()) {
      testDirectories(compressedTarFileExtension);
    }
  }

  public void testDirectories(String compressedTarFileExtension)
      throws IOException, CompressorException {
    String dirToTarName1 = "dir1";
    String dirToTarName2 = "dir2";
    File dir1 = new File(DATA_DIR, dirToTarName1);
    File dir2 = new File(DATA_DIR, dirToTarName2);

    File[] dirsToTar = new File[]{dir1, dir2};

    String fileName1 = "data1";
    String fileContent1 = "fileContent1";
    String fileName2 = "data2";
    String fileContent2 = "fileContent2";
    FileUtils.write(new File(dir1, fileName1), fileContent1);
    FileUtils.write(new File(dir2, fileName2), fileContent2);

    String outputTarName = "output_tar" + compressedTarFileExtension;
    File compressedTarFile = new File(TAR_DIR, outputTarName);
    TarCompressionUtils.createCompressedTarFile(dirsToTar, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 4);

    /*
    untarredFiles ends up being a list as follows:
    /dir1/
    /dir1/data1
    /dir2/
    /dir2/data2

    To fetch the 2 directories we want for the following assertions, we expect them at indexes 0 and 2.
     */
    File untarredFileDir1 = untarredFiles.get(0);
    File untarredFileDir2 = untarredFiles.get(2);

    assertEquals(untarredFileDir1, new File(UNTAR_DIR, dirToTarName1));
    assertEquals(untarredFileDir2, new File(UNTAR_DIR, dirToTarName2));

    File[] filesDir1 = untarredFileDir1.listFiles();
    assertNotNull(filesDir1);
    assertEquals(filesDir1.length, 1);
    assertEquals(FileUtils.readFileToString(new File(untarredFileDir1, fileName1)), fileContent1);

    File[] filesDir2 = untarredFileDir2.listFiles();
    assertNotNull(filesDir2);
    assertEquals(filesDir2.length, 1);
    assertEquals(FileUtils.readFileToString(new File(untarredFileDir2, fileName2)), fileContent2);
  }

  @Test
  public void testDirectory()
      throws IOException, CompressorException {
    for (String compressedTarFileExtension : TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.keySet()) {
      testDirectory(compressedTarFileExtension);
    }
  }

  public void testDirectory(String compressedTarFileExtension)
      throws IOException, CompressorException {
    String dirName = "dir";
    File dir = new File(DATA_DIR, dirName);
    String fileName1 = "data1";
    String fileContent1 = "fileContent1";
    String fileName2 = "data2";
    String fileContent2 = "fileContent2";
    FileUtils.write(new File(dir, fileName1), fileContent1);
    FileUtils.write(new File(dir, fileName2), fileContent2);

    File compressedTarFile = new File(TAR_DIR, dirName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dir, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 3);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    assertEquals(FileUtils.readFileToString(new File(untarredFile, fileName1)), fileContent1);
    assertEquals(FileUtils.readFileToString(new File(untarredFile, fileName2)), fileContent2);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName1, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent1);
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName2, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent2);
    try {
      TarCompressionUtils.untarOneFile(compressedTarFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testSubDirectories()
      throws IOException, CompressorException {
    for (String compressedTarFileExtension : TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.keySet()) {
      testSubDirectories(compressedTarFileExtension);
    }
  }

  public void testSubDirectories(String compressedTarFileExtension)
      throws IOException {
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

    File compressedTarFile = new File(TAR_DIR, dirName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dir, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 5);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    assertEquals(FileUtils.readFileToString(new File(new File(untarredFile, subDirName1), fileName1)), fileContent1);
    assertEquals(FileUtils.readFileToString(new File(new File(untarredFile, subDirName2), fileName2)), fileContent2);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName1, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent1);
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName2, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent2);
    try {
      TarCompressionUtils.untarOneFile(compressedTarFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
    try {
      TarCompressionUtils.untarOneFile(compressedTarFile, subDirName1, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
    try {
      TarCompressionUtils.untarOneFile(compressedTarFile, subDirName2, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testEmptyDirectory()
      throws IOException, CompressorException {
    for (String compressedTarFileExtension : TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.keySet()) {
      testEmptyDirectory(compressedTarFileExtension);
    }
  }

  public void testEmptyDirectory(String compressedTarFileExtension)
      throws IOException {
    String dirName = "dir";
    File dir = new File(DATA_DIR, dirName);
    FileUtils.forceMkdir(dir);

    File compressedTarFile = new File(TAR_DIR, dirName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dir, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 1);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 0);

    untarredFile = new File(UNTAR_DIR, "untarred");
    try {
      TarCompressionUtils.untarOneFile(compressedTarFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testBadFilePath()
      throws IOException {
    for (Map.Entry<String, String> entry : TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.entrySet()) {
      testBadFilePath(entry.getKey(), entry.getValue());
    }
  }

  public void testBadFilePath(String compressedTarFileExtension, String compressorName)
      throws IOException {
    String fileName = "data";
    String fileContent = "fileContent";
    File dataFile = new File(DATA_DIR, fileName);
    FileUtils.write(dataFile, fileContent);

    File badCompressedTarFile = new File(TAR_DIR, "bad" + compressedTarFileExtension);
    try (OutputStream fileOut = Files.newOutputStream(badCompressedTarFile.toPath());
        OutputStream compressorOut = COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressorName, fileOut);
        TarArchiveOutputStream tarOut = new TarArchiveOutputStream(compressorOut)) {
      tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
      TarArchiveEntry entry = new TarArchiveEntry(dataFile, "../bad/path/data");
      tarOut.putArchiveEntry(entry);
      try (InputStream in = Files.newInputStream(dataFile.toPath())) {
        IOUtils.copy(in, tarOut);
      }
      tarOut.closeArchiveEntry();
    } catch (CompressorException e) {
      throw new IOException(e);
    }

    try {
      TarCompressionUtils.untar(badCompressedTarFile, UNTAR_DIR);
      fail();
    } catch (IOException e) {
      // Expected
    }

    // Allow untar one file to the given destination
    File untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(badCompressedTarFile, fileName, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile), fileContent);
  }
}
