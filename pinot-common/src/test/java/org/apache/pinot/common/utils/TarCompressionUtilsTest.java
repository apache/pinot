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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.compression.Lz4CompressorStreamProvider.LZ4_FAST;
import static org.apache.pinot.common.compression.Lz4CompressorStreamProvider.LZ4_HC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
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
    testFile(TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION);
  }

  public void testFile(String compressedTarFileExtension)
      throws IOException, CompressorException {
    String fileName = "data";
    String fileContent = "fileContent";
    File dataFile = new File(DATA_DIR, fileName);
    FileUtils.write(dataFile, fileContent, StandardCharsets.UTF_8);

    File compressedTarFile = new File(TAR_DIR, fileName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dataFile, compressedTarFile);
    if (compressedTarFileExtension.equals(TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION)) {
      try (InputStream input = new BufferedInputStream(Files.newInputStream(compressedTarFile.toPath()))) {
        assertEquals(CompressorStreamFactory.detect(input), CompressorStreamFactory.GZIP);
      }
    }

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 1);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, fileName));
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent);
  }

  @DataProvider(name = "lz4Implementations")
  public Object[][] lz4Implementations() {
    return new Object[][]{{CompressorStreamFactory.LZ4_FRAMED}, {LZ4_FAST}, {LZ4_HC}};
  }

  @Test(dataProvider = "lz4Implementations")
  public void testLz4ArchiveRoundTrip(String compressorName)
      throws IOException {
    int contentSize = compressorName.equals(CompressorStreamFactory.LZ4_FRAMED) ? 32 * 1024 : 5 * 1024 * 1024;
    byte[] content = new byte[contentSize];
    new Random(0).nextBytes(content);
    Arrays.fill(content, content.length / 5, content.length * 3 / 5, (byte) 42);
    File inputFile = new File(DATA_DIR, "data");
    Files.write(inputFile.toPath(), content);
    File archive = new File(TAR_DIR, "data" + TarCompressionUtils.TAR_LZ4_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(inputFile, archive, compressorName);
    if (compressorName.equals(LZ4_FAST)) {
      assertEquals(TarCompressionUtils.COMPRESSOR_NAME_BY_FILE_EXTENSIONS.get(
          TarCompressionUtils.TAR_LZ4_FILE_EXTENSION), LZ4_FAST);
      File defaultArchive = new File(TAR_DIR, "default" + TarCompressionUtils.TAR_LZ4_FILE_EXTENSION);
      TarCompressionUtils.createCompressedTarFile(inputFile, defaultArchive);
      assertEquals(Files.readAllBytes(TarCompressionUtils.untar(defaultArchive, UNTAR_DIR).get(0).toPath()), content);
    }

    List<File> extractedFiles = TarCompressionUtils.untar(archive, UNTAR_DIR);
    assertEquals(extractedFiles.size(), 1);
    assertEquals(Files.readAllBytes(extractedFiles.get(0).toPath()), content);
    File extractedFile = new File(UNTAR_DIR, "single");
    TarCompressionUtils.untarOneFile(archive, inputFile.getName(), extractedFile);
    assertEquals(Files.readAllBytes(extractedFile.toPath()), content);
  }

  @Test
  public void testLz4FactoryFinish()
      throws IOException, CompressorException {
    boolean[] closed = {false};
    ByteArrayOutputStream sink = new ByteArrayOutputStream() {
      @Override
      public void close() {
        closed[0] = true;
      }
    };
    byte[] content = "factory stream".getBytes(StandardCharsets.UTF_8);
    try (CompressorOutputStream<?> output =
        COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(LZ4_FAST, sink)) {
      output.write(content[0]);
      output.write(content, 1, content.length - 1);
      output.finish();
      assertFalse(closed[0]);
      try (InputStream input = COMPRESSOR_STREAM_FACTORY.createCompressorInputStream(LZ4_FAST,
          new ByteArrayInputStream(sink.toByteArray()))) {
        assertEquals(IOUtils.toByteArray(input), content);
      }
    }
    assertTrue(closed[0]);
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
    FileUtils.write(new File(dir1, fileName1), fileContent1, StandardCharsets.UTF_8);
    FileUtils.write(new File(dir2, fileName2), fileContent2, StandardCharsets.UTF_8);

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
    assertEquals(
        FileUtils.readFileToString(new File(untarredFileDir1, fileName1), StandardCharsets.UTF_8), fileContent1);

    File[] filesDir2 = untarredFileDir2.listFiles();
    assertNotNull(filesDir2);
    assertEquals(filesDir2.length, 1);
    assertEquals(
        FileUtils.readFileToString(new File(untarredFileDir2, fileName2), StandardCharsets.UTF_8), fileContent2);
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
    FileUtils.write(new File(dir, fileName1), fileContent1, StandardCharsets.UTF_8);
    FileUtils.write(new File(dir, fileName2), fileContent2, StandardCharsets.UTF_8);

    File compressedTarFile = new File(TAR_DIR, dirName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dir, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 3);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    assertEquals(FileUtils.readFileToString(new File(untarredFile, fileName1), StandardCharsets.UTF_8), fileContent1);
    assertEquals(FileUtils.readFileToString(new File(untarredFile, fileName2), StandardCharsets.UTF_8), fileContent2);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName1, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent1);
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName2, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent2);
    try {
      TarCompressionUtils.untarOneFile(compressedTarFile, dirName, untarredFile);
      fail();
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testFilteredDirectory()
      throws IOException {
    File dir = new File(DATA_DIR, "dir");
    File excludedFile = new File(dir, "segment_metadata.bin");
    File nestedFile = new File(new File(dir, "nested"), "segment_metadata.bin");
    FileUtils.write(excludedFile, "local", StandardCharsets.UTF_8);
    FileUtils.write(new File(dir, "metadata.properties"), "canonical", StandardCharsets.UTF_8);
    FileUtils.write(nestedFile, "nested", StandardCharsets.UTF_8);

    File compressedTarFile = new File(TAR_DIR, "filtered.tar.gz");
    TarCompressionUtils.createFilteredCompressedTarFile(dir, compressedTarFile, file -> !file.equals(excludedFile));
    TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);

    File untarredDir = new File(UNTAR_DIR, "dir");
    assertTrue(untarredDir.isDirectory());
    assertFalse(new File(untarredDir, excludedFile.getName()).exists());
    assertTrue(new File(untarredDir, "metadata.properties").isFile());
    assertTrue(new File(new File(untarredDir, "nested"), nestedFile.getName()).isFile());
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
    FileUtils.write(new File(subDir1, fileName1), fileContent1, StandardCharsets.UTF_8);
    FileUtils.write(new File(subDir2, fileName2), fileContent2, StandardCharsets.UTF_8);

    File compressedTarFile = new File(TAR_DIR, dirName + compressedTarFileExtension);
    TarCompressionUtils.createCompressedTarFile(dir, compressedTarFile);

    List<File> untarredFiles = TarCompressionUtils.untar(compressedTarFile, UNTAR_DIR);
    assertEquals(untarredFiles.size(), 5);
    File untarredFile = untarredFiles.get(0);
    assertEquals(untarredFile, new File(UNTAR_DIR, dirName));
    File[] files = untarredFile.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    assertEquals(FileUtils.readFileToString(new File(new File(untarredFile, subDirName1), fileName1),
        StandardCharsets.UTF_8), fileContent1);
    assertEquals(FileUtils.readFileToString(new File(new File(untarredFile, subDirName2), fileName2),
        StandardCharsets.UTF_8), fileContent2);

    untarredFile = new File(UNTAR_DIR, "untarred");
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName1, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent1);
    TarCompressionUtils.untarOneFile(compressedTarFile, fileName2, untarredFile);
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent2);
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
    FileUtils.write(dataFile, fileContent, StandardCharsets.UTF_8);

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
    assertEquals(FileUtils.readFileToString(untarredFile, StandardCharsets.UTF_8), fileContent);
  }
}
