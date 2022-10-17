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
package org.apache.pinot.plugin.filesystem.test;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.filesystem.AzurePinotFS;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests the Azure implementation of PinotFS
 */
public class AzurePinotFSTest {
  private String _adlLocation;
  private File _testFile;

  @BeforeMethod
  public void setup()
      throws IOException {
    _adlLocation =
        new File(System.getProperty("java.io.tmpdir"), AzurePinotFSTest.class.getSimpleName()).getAbsolutePath();
    FileUtils.deleteQuietly(new File(_adlLocation));
    Assert.assertTrue(new File(_adlLocation).mkdir(), "Could not make directory" + _adlLocation);

    try {
      _testFile = new File(_adlLocation, "testFile");
      Assert.assertTrue(_testFile.createNewFile(), "Could not create file " + _testFile.getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    new File(_adlLocation).deleteOnExit();
  }

  @Test
  public void testFS()
      throws Exception {
    ADLStoreClient adlStoreClient = Mockito.mock(ADLStoreClient.class);
    ADLFileInputStream adlFileInputStream = Mockito.mock(ADLFileInputStream.class);
    Mockito.when(adlStoreClient.checkExists(_adlLocation)).thenReturn(true);
    Mockito.when(adlStoreClient.checkExists(_testFile.getPath())).thenReturn(true);

    AzurePinotFS azurePinotFS = new AzurePinotFS(adlStoreClient);
    URI testFileURI = _testFile.toURI();
    Assert.assertTrue(azurePinotFS.exists(testFileURI));
    Assert.assertTrue(azurePinotFS.exists(new URI(_adlLocation)));

    File file = new File(_adlLocation, "testfile2");
    Mockito.when(adlStoreClient.getReadStream(ArgumentMatchers.anyString())).thenReturn(adlFileInputStream);
    azurePinotFS.copyToLocalFile(testFileURI, file);
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testListFilesWithMetadataNonRecursive()
      throws IOException {
    ADLStoreClient adlStoreClient = Mockito.mock(ADLStoreClient.class);
    AzurePinotFS azurePinotFS = new AzurePinotFS(adlStoreClient);
    URI baseURI = new File(_adlLocation, "testDirNonRecursive").toURI();

    Date lastModifiedTime = new Date();
    Date lastAccessTime = new Date();
    DirectoryEntry testDir =
        new DirectoryEntry("testDir", baseURI.toString(), 1024, null, null, lastAccessTime, lastModifiedTime,
            DirectoryEntryType.DIRECTORY, 128, 1, null, false, null);
    Mockito.when(adlStoreClient.getDirectoryEntry(baseURI.getPath())).thenReturn(testDir);

    List<DirectoryEntry> entries = new ArrayList<>();
    DirectoryEntry dir =
        new DirectoryEntry("dir1", testDir.fullName + "/dir1", 128, null, null, lastAccessTime, lastModifiedTime,
            DirectoryEntryType.DIRECTORY, 128, 1, null, false, null);
    entries.add(dir);
    DirectoryEntry file =
        new DirectoryEntry("file1", testDir.fullName + "/file1", 1024, null, null, lastAccessTime, lastModifiedTime,
            DirectoryEntryType.FILE, 128, 1, null, false, null);
    entries.add(file);
    Mockito.when(adlStoreClient.enumerateDirectory(Mockito.anyString())).thenReturn(entries);

    String[] files = azurePinotFS.listFiles(baseURI, false);
    Assert.assertEquals(files.length, 2);
    Assert
        .assertTrue(entries.stream().map(d -> d.fullName).collect(Collectors.toSet()).containsAll(Arrays.asList(files)),
            Arrays.toString(files));

    List<FileMetadata> fileMetadata = azurePinotFS.listFilesWithMetadata(baseURI, false);
    Assert.assertEquals(fileMetadata.size(), 2);
    Assert.assertTrue(entries.stream().map(d -> d.fullName).collect(Collectors.toSet())
            .containsAll(fileMetadata.stream().map(FileMetadata::getFilePath).collect(Collectors.toSet())),
        fileMetadata.toString());
    Assert.assertEquals(fileMetadata.stream().filter(FileMetadata::isDirectory).count(), 1);
    Assert.assertEquals(fileMetadata.stream().filter(f -> !f.isDirectory()).count(), 1);
  }

  @Test
  public void testListFilesWithMetadataRecursive()
      throws IOException {
    ADLStoreClient adlStoreClient = Mockito.mock(ADLStoreClient.class);
    AzurePinotFS azurePinotFS = new AzurePinotFS(adlStoreClient);
    URI baseURI = new File(_adlLocation, "testDirRecursive").toURI();

    Date lastModifiedTime = new Date();
    Date lastAccessTime = new Date();
    DirectoryEntry testDir =
        new DirectoryEntry("testDir", baseURI.toString(), 128, null, null, lastAccessTime, lastModifiedTime,
            DirectoryEntryType.DIRECTORY, 128, 1, null, false, null);
    Mockito.when(adlStoreClient.getDirectoryEntry(baseURI.getPath())).thenReturn(testDir);

    Set<String> expected = new HashSet<>();
    List<DirectoryEntry> dirEntries = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      DirectoryEntry dir = new DirectoryEntry("dir" + i, testDir.fullName + "/dir" + i, 128, null, null, lastAccessTime,
          lastModifiedTime, DirectoryEntryType.DIRECTORY, 128, 1, null, false, null);
      dirEntries.add(dir);
      expected.add(dir.fullName);
    }
    List<DirectoryEntry> fileEntries = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DirectoryEntry file =
          new DirectoryEntry("file" + i, testDir.fullName + "/dir2/file" + i, 1024, null, null, lastAccessTime,
              lastModifiedTime, DirectoryEntryType.FILE, 128, 1, null, false, null);
      fileEntries.add(file);
      expected.add(file.fullName);
    }

    // Prepare the mock for calling listFiles() and listFilesWithMetadata().
    Mockito.when(adlStoreClient.enumerateDirectory(Mockito.anyString()))
        .thenReturn(dirEntries, Collections.emptyList(), fileEntries /* for listFiles() */, dirEntries,
            Collections.emptyList(), fileEntries /* for listFilesWithMetadata() */);

    String[] files = azurePinotFS.listFiles(baseURI, true);
    Assert.assertEquals(files.length, 5);
    Assert.assertTrue(expected.containsAll(Arrays.asList(files)), Arrays.toString(files));

    List<FileMetadata> fileMetadata = azurePinotFS.listFilesWithMetadata(baseURI, true);
    Assert.assertEquals(fileMetadata.size(), 5);
    Assert.assertTrue(
        expected.containsAll(fileMetadata.stream().map(FileMetadata::getFilePath).collect(Collectors.toSet())),
        fileMetadata.toString());
    Assert.assertEquals(fileMetadata.stream().filter(FileMetadata::isDirectory).count(), 2);
    Assert.assertEquals(fileMetadata.stream().filter(f -> !f.isDirectory()).count(), 3);
  }

  @AfterClass
  public void tearDown() {
    new File(_adlLocation).delete();
  }
}
