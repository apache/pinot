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

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.SimpleResponse;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS;
import org.apache.pinot.plugin.filesystem.AzurePinotFSUtil;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.utils.PinotMd5Mode;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


/**
 * Tests the Azure implementation of ADLSGen2PinotFS
 */
public class ADLSGen2PinotFSTest {

  @Mock
  private DataLakeFileSystemClient _mockFileSystemClient;
  @Mock
  private DataLakeDirectoryClient _mockDirectoryClient;
  @Mock
  private DataLakeServiceClient _mockServiceClient;
  @Mock
  private DataLakeFileClient _mockFileClient;
  @Mock
  private DataLakeFileOpenInputStreamResult _mockFileOpenInputStreamResult;
  @Mock
  private InputStream _mockInputStream;
  @Mock
  private DataLakeStorageException _mockDataLakeStorageException;
  @Mock
  private SimpleResponse _mockSimpleResponse;
  @Mock
  private PathProperties _mockPathProperties;
  @Mock
  private PagedIterable _mockPagedIterable;
  @Mock
  private PathItem _mockPathItem;
  @Mock
  private BlobContainerClient _mockBlobContainerClient;
  @Mock
  private BlobClient _mockBlobClient;

  private URI _mockURI;
  private ADLSGen2PinotFS _adlsGen2PinotFsUnderTest;

  private final static String MOCK_FILE_SYSTEM_NAME = "fileSystemName";

  @BeforeMethod
  public void setup()
      throws URISyntaxException {
    MockitoAnnotations.openMocks(this);
    _adlsGen2PinotFsUnderTest = new ADLSGen2PinotFS(_mockFileSystemClient);
    _mockURI = new URI("mock://mock");
  }

  @AfterMethod
  public void tearDown() {
    verifyNoMoreInteractions(_mockDataLakeStorageException, _mockServiceClient, _mockFileSystemClient,
        _mockSimpleResponse, _mockDirectoryClient, _mockPathItem, _mockPagedIterable, _mockPathProperties,
        _mockFileClient, _mockFileOpenInputStreamResult, _mockInputStream, _mockBlobContainerClient, _mockBlobClient);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testInitNoAuth() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    _adlsGen2PinotFsUnderTest.init(pinotConfiguration);
  }

  @Test
  public void testSasTokenAuthentication() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty("authenticationType", "SAS_TOKEN");
    pinotConfiguration.setProperty("sasToken", "sp=rwdl&se=2025-12-31T23:59:59Z&sv=2022-11-02&sr=c&sig=test");
    pinotConfiguration.setProperty("accountName", "testaccount");
    pinotConfiguration.setProperty("fileSystemName", "testcontainer");

    when(_mockServiceClient.getFileSystemClient("testcontainer")).thenReturn(_mockFileSystemClient);
    when(_mockFileSystemClient.getProperties()).thenReturn(null);

    // Mock the creation of the service client
    ADLSGen2PinotFS sasTokenFS = new ADLSGen2PinotFS() {
      @Override
      public DataLakeFileSystemClient getOrCreateClientWithFileSystem(DataLakeServiceClient serviceClient,
          String fileSystemName) {
        return _mockFileSystemClient;
      }
    };

    sasTokenFS.init(pinotConfiguration);

    // Verify that the filesystem client was set properly
    assertTrue(sasTokenFS != null);
  }

  @Test
  public void testChecksumEnabledWithMd5DisabledFails() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty("authenticationType", "SAS_TOKEN");
    pinotConfiguration.setProperty("sasToken", "sp=rwdl&se=2025-12-31T23:59:59Z&sv=2022-11-02&sr=c&sig=test");
    pinotConfiguration.setProperty("accountName", "testaccount");
    pinotConfiguration.setProperty("fileSystemName", "testcontainer");
    pinotConfiguration.setProperty("enableChecksum", "true");

    ADLSGen2PinotFS adlsGen2PinotFs = new ADLSGen2PinotFS() {
      @Override
      public DataLakeFileSystemClient getOrCreateClientWithFileSystem(DataLakeServiceClient serviceClient,
          String fileSystemName) {
        return _mockFileSystemClient;
      }
    };

    try {
      PinotMd5Mode.setPinotMd5Disabled(true);
      IllegalStateException exception =
          expectThrows(IllegalStateException.class, () -> adlsGen2PinotFs.init(pinotConfiguration));
      assertTrue(exception.getMessage().contains("pinot.md5.disabled"));
      assertTrue(exception.getMessage().contains("enableChecksum"));
    } finally {
      PinotMd5Mode.setPinotMd5Disabled(false);
    }
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testSasTokenMissingToken() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty("authenticationType", "SAS_TOKEN");
    pinotConfiguration.setProperty("accountName", "testaccount");
    pinotConfiguration.setProperty("fileSystemName", "testcontainer");
    // Missing sasToken property

    _adlsGen2PinotFsUnderTest.init(pinotConfiguration);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testSasTokenNullToken() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty("authenticationType", "SAS_TOKEN");
    pinotConfiguration.setProperty("sasToken", (String) null);
    pinotConfiguration.setProperty("accountName", "testaccount");
    pinotConfiguration.setProperty("fileSystemName", "testcontainer");

    _adlsGen2PinotFsUnderTest.init(pinotConfiguration);
  }

  @Test
  public void testGetOrCreateClientWithFileSystemGet() {
    when(_mockServiceClient.getFileSystemClient(MOCK_FILE_SYSTEM_NAME)).thenReturn(_mockFileSystemClient);
    when(_mockFileSystemClient.getProperties()).thenReturn(null);

    final DataLakeFileSystemClient actual =
        _adlsGen2PinotFsUnderTest.getOrCreateClientWithFileSystem(_mockServiceClient, MOCK_FILE_SYSTEM_NAME);
    assertEquals(actual, _mockFileSystemClient);

    verify(_mockFileSystemClient).getProperties();
    verify(_mockServiceClient).getFileSystemClient(MOCK_FILE_SYSTEM_NAME);
  }

  @Test
  public void testGetOrCreateClientWithFileSystemCreate() {
    when(_mockServiceClient.getFileSystemClient(MOCK_FILE_SYSTEM_NAME)).thenReturn(_mockFileSystemClient);
    when(_mockServiceClient.createFileSystem(MOCK_FILE_SYSTEM_NAME)).thenReturn(_mockFileSystemClient);
    when(_mockFileSystemClient.getProperties()).thenThrow(_mockDataLakeStorageException);
    when(_mockDataLakeStorageException.getStatusCode()).thenReturn(404);
    when(_mockDataLakeStorageException.getErrorCode()).thenReturn("ContainerNotFound");

    final DataLakeFileSystemClient actual =
        _adlsGen2PinotFsUnderTest.getOrCreateClientWithFileSystem(_mockServiceClient, MOCK_FILE_SYSTEM_NAME);
    assertEquals(actual, _mockFileSystemClient);

    verify(_mockFileSystemClient).getProperties();
    verify(_mockServiceClient).getFileSystemClient(MOCK_FILE_SYSTEM_NAME);
    verify(_mockServiceClient).createFileSystem(MOCK_FILE_SYSTEM_NAME);
    verify(_mockDataLakeStorageException).getStatusCode();
    verify(_mockDataLakeStorageException).getErrorCode();
  }

  @Test
  public void testMkDirHappy()
      throws IOException {
    when(_mockFileSystemClient.createDirectoryWithResponse(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(_mockSimpleResponse);

    boolean actual = _adlsGen2PinotFsUnderTest.mkdir(_mockURI);
    assertTrue(actual);

    verify(_mockFileSystemClient).createDirectoryWithResponse(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testMkDirPathExists()
      throws IOException {
    when(_mockFileSystemClient.createDirectoryWithResponse(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(_mockDataLakeStorageException);
    when(_mockDataLakeStorageException.getStatusCode()).thenReturn(409);
    when(_mockDataLakeStorageException.getErrorCode()).thenReturn("PathAlreadyExists");

    boolean actual = _adlsGen2PinotFsUnderTest.mkdir(_mockURI);
    assertTrue(actual);

    verify(_mockFileSystemClient).createDirectoryWithResponse(any(), any(), any(), any(), any(), any(), any(), any());
    verify(_mockDataLakeStorageException).getStatusCode();
    verify(_mockDataLakeStorageException).getErrorCode();
  }

  @Test
  public void testIsDirectory()
      throws IOException {
    final HashMap<String, String> metadata = new HashMap<>();
    metadata.put("hdi_isfolder", "true");

    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);
    when(_mockPathProperties.getMetadata()).thenReturn(metadata);

    boolean actual = _adlsGen2PinotFsUnderTest.isDirectory(_mockURI);
    assertTrue(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getMetadata();
  }

  @Test
  public void testListFiles()
      throws IOException {
    when(_mockFileSystemClient.listPaths(any(), any())).thenReturn(_mockPagedIterable);
    when(_mockPagedIterable.stream()).thenReturn(Stream.of(_mockPathItem));
    when(_mockPathItem.getName()).thenReturn("foo");

    String[] actual = _adlsGen2PinotFsUnderTest.listFiles(_mockURI, true);
    assertEquals(actual[0], "/foo");

    verify(_mockFileSystemClient).listPaths(any(), any());
    verify(_mockPagedIterable).stream();
    verify(_mockPathItem).getName();
  }

  @Test
  public void testListFilesWithMetadata()
      throws IOException {
    when(_mockFileSystemClient.listPaths(any(), any())).thenReturn(_mockPagedIterable);
    when(_mockPagedIterable.stream()).thenReturn(Stream.of(_mockPathItem));
    when(_mockPathItem.getName()).thenReturn("foo");
    when(_mockPathItem.isDirectory()).thenReturn(false);
    when(_mockPathItem.getContentLength()).thenReturn(1024L);
    OffsetDateTime mtime = OffsetDateTime.now();
    when(_mockPathItem.getLastModified()).thenReturn(mtime);

    List<FileMetadata> actual = _adlsGen2PinotFsUnderTest.listFilesWithMetadata(_mockURI, true);
    FileMetadata fm = actual.get(0);
    assertEquals(fm.getFilePath(), "/foo");
    assertFalse(fm.isDirectory());
    assertEquals(fm.getLength(), 1024);
    assertEquals(fm.getLastModifiedTime(), mtime.toInstant().toEpochMilli());

    verify(_mockFileSystemClient).listPaths(any(), any());
    verify(_mockPagedIterable).stream();
    verify(_mockPathItem).getName();
    verify(_mockPathItem).isDirectory();
    verify(_mockPathItem).getContentLength();
    verify(_mockPathItem).getLastModified();
  }

  @Test
  public void testLastModified()
      throws IOException {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);
    Instant now = Instant.now();
    OffsetDateTime mtime = now.atOffset(ZoneOffset.UTC);
    when(_mockPathProperties.getLastModified()).thenReturn(mtime);

    long actual = _adlsGen2PinotFsUnderTest.lastModified(_mockURI);
    assertEquals(actual, now.toEpochMilli());

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getLastModified();
  }

  @Test
  public void testListFilesException() {
    when(_mockFileSystemClient.listPaths(any(), any())).thenThrow(_mockDataLakeStorageException);

    expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.listFiles(_mockURI, true));

    verify(_mockFileSystemClient).listPaths(any(), any());
  }

  @Test
  public void testDeleteDirectory()
      throws IOException {
    final HashMap<String, String> metadata = new HashMap<>();
    metadata.put("hdi_isfolder", "true");

    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);
    when(_mockPathProperties.getMetadata()).thenReturn(metadata);
    when(_mockFileSystemClient.listPaths(any(), any())).thenReturn(_mockPagedIterable);
    when(_mockPagedIterable.stream()).thenReturn(Stream.of(_mockPathItem));
    when(_mockPathItem.getName()).thenReturn("foo");
    when(_mockFileSystemClient.deleteDirectoryWithResponse(eq(""), eq(true), eq(null), eq(null), eq(Context.NONE)))
        .thenReturn(_mockSimpleResponse);
    when(_mockSimpleResponse.getValue()).thenReturn(null);

    boolean actual = _adlsGen2PinotFsUnderTest.delete(_mockURI, true);
    assertTrue(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getMetadata();
    verify(_mockFileSystemClient).listPaths(any(), any());
    verify(_mockPagedIterable).stream();
    verify(_mockPathItem).getName();
    verify(_mockFileSystemClient).deleteDirectoryWithResponse(eq(""), eq(true), eq(null), eq(null), eq(Context.NONE));
    verify(_mockSimpleResponse).getValue();
  }

  @Test
  public void testDeleteFile()
      throws IOException {
    final HashMap<String, String> metadata = new HashMap<>();
    metadata.put("hdi_isfolder", "false");

    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);
    when(_mockPathProperties.getMetadata()).thenReturn(metadata);
    doNothing().when(_mockFileSystemClient).deleteFile(any());

    boolean actual = _adlsGen2PinotFsUnderTest.delete(_mockURI, true);
    assertTrue(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getMetadata();
    verify(_mockFileSystemClient).deleteFile(any());
  }

  @Test
  public void testDoMove()
      throws IOException {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.rename(eq(null), any())).thenReturn(_mockDirectoryClient);

    boolean actual = _adlsGen2PinotFsUnderTest.doMove(_mockURI, _mockURI);
    assertTrue(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).rename(eq(null), any());
  }

  @Test
  public void testDoMoveException() {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenThrow(_mockDataLakeStorageException);

    expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.doMove(_mockURI, _mockURI));

    verify(_mockFileSystemClient).getDirectoryClient(any());
  }

  @Test
  public void testExistsTrue()
      throws IOException {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);

    boolean actual = _adlsGen2PinotFsUnderTest.exists(_mockURI);
    assertTrue(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
  }

  @Test
  public void testExistsFalse()
      throws IOException {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenThrow(_mockDataLakeStorageException);
    when(_mockDataLakeStorageException.getStatusCode()).thenReturn(404);

    boolean actual = _adlsGen2PinotFsUnderTest.exists(_mockURI);
    assertFalse(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockDataLakeStorageException).getStatusCode();
  }

  @Test
  public void testExistsException() {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenThrow(_mockDataLakeStorageException);
    when(_mockDataLakeStorageException.getStatusCode()).thenReturn(123);

    expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.exists(_mockURI));

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockDataLakeStorageException).getStatusCode();
  }

  @Test
  public void testLength()
      throws IOException {
    final long testLength = 42;
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);
    when(_mockPathProperties.getFileSize()).thenReturn(testLength);

    long actual = _adlsGen2PinotFsUnderTest.length(_mockURI);
    assertEquals(actual, testLength);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getFileSize();
  }

  @Test
  public void testLengthException() {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenThrow(_mockDataLakeStorageException);

    expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.length(_mockURI));

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
  }

  @Test
  public void testTouch()
      throws IOException {
    when(_mockFileSystemClient.getFileClient(any())).thenReturn(_mockFileClient);
    when(_mockFileClient.getProperties()).thenReturn(_mockPathProperties);
    doNothing().when(_mockFileClient).setHttpHeaders(any());

    boolean actual = _adlsGen2PinotFsUnderTest.touch(_mockURI);
    assertTrue(actual);

    verify(_mockFileSystemClient).getFileClient(any());
    verify(_mockFileClient).getProperties();
    verify(_mockFileClient).setHttpHeaders(any());
    verify(_mockPathProperties).getCacheControl();
    verify(_mockPathProperties).getContentDisposition();
    verify(_mockPathProperties).getContentEncoding();
    verify(_mockPathProperties).getContentMd5();
    verify(_mockPathProperties).getContentLanguage();
    verify(_mockPathProperties).getContentType();
  }

  @Test
  public void testTouchException() {
    when(_mockFileSystemClient.getFileClient(any())).thenReturn(_mockFileClient);
    when(_mockFileClient.getProperties()).thenReturn(_mockPathProperties);
    doThrow(_mockDataLakeStorageException).when(_mockFileClient).setHttpHeaders(any());

    expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.touch(_mockURI));

    verify(_mockFileSystemClient).getFileClient(any());
    verify(_mockFileClient).getProperties();
    verify(_mockFileClient).setHttpHeaders(any());
    verify(_mockPathProperties).getCacheControl();
    verify(_mockPathProperties).getContentDisposition();
    verify(_mockPathProperties).getContentEncoding();
    verify(_mockPathProperties).getContentMd5();
    verify(_mockPathProperties).getContentLanguage();
    verify(_mockPathProperties).getContentType();
  }

  @Test
  public void open()
      throws IOException {
    when(_mockFileSystemClient.getFileClient(any())).thenReturn(_mockFileClient);
    when(_mockFileClient.openInputStream()).thenReturn(_mockFileOpenInputStreamResult);
    when(_mockFileOpenInputStreamResult.getInputStream()).thenReturn(_mockInputStream);

    InputStream actual = _adlsGen2PinotFsUnderTest.open(_mockURI);
    assertEquals(actual, _mockInputStream);

    verify(_mockFileSystemClient).getFileClient(AzurePinotFSUtil.convertUriToAzureStylePath(_mockURI));
    verify(_mockFileClient).openInputStream();
    verify(_mockFileOpenInputStreamResult).getInputStream();
  }

  @Test
  public void testCopyToLocalFileWithSubdirectories() throws Exception {
    // Create a temporary file for the test
    File tempDir = new File(System.getProperty("java.io.tmpdir"), "pinot_test");
    tempDir.mkdirs();
    File mockDstFile = new File(tempDir, "test_file.txt");

    // Create parent directory
    File parentFile = mockDstFile.getParentFile();
    if (!parentFile.exists()) {
      parentFile.mkdirs();
    }

    // Mock file stream
    byte[] testData = "test data".getBytes();
    InputStream mockInputStream = new ByteArrayInputStream(testData);
    when(_mockFileSystemClient.getFileClient(any())).thenReturn(_mockFileClient);
    when(_mockFileClient.openInputStream()).thenReturn(_mockFileOpenInputStreamResult);
    when(_mockFileOpenInputStreamResult.getInputStream()).thenReturn(mockInputStream);

    try {
      // Execute
      _adlsGen2PinotFsUnderTest.copyToLocalFile(_mockURI, mockDstFile);

      // Verify file operations in order
      verify(_mockFileSystemClient).getFileClient(AzurePinotFSUtil.convertUriToAzureStylePath(_mockURI));
      verify(_mockFileClient).openInputStream();
      verify(_mockFileOpenInputStreamResult).getInputStream();

      // Verify file was created
      assertTrue(mockDstFile.exists());

      // Verify content was written correctly
      byte[] writtenContent = Files.readAllBytes(mockDstFile.toPath());
      assertArrayEquals(testData, writtenContent);
    } finally {
      // Cleanup
      FileUtils.deleteQuietly(mockDstFile);
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testCopyToLocalFileWithoutSubdirectories() throws Exception {
    // Create a temporary file for the test
    File tempFile = new File(System.getProperty("java.io.tmpdir"), "test_file.txt");

    // Mock file stream
    byte[] testData = "test data".getBytes();
    InputStream mockInputStream = new ByteArrayInputStream(testData);
    when(_mockFileSystemClient.getFileClient(any())).thenReturn(_mockFileClient);
    when(_mockFileClient.openInputStream()).thenReturn(_mockFileOpenInputStreamResult);
    when(_mockFileOpenInputStreamResult.getInputStream()).thenReturn(mockInputStream);

    try {
      // Execute
      _adlsGen2PinotFsUnderTest.copyToLocalFile(_mockURI, tempFile);

      // Verify file operations in order
      verify(_mockFileSystemClient).getFileClient(AzurePinotFSUtil.convertUriToAzureStylePath(_mockURI));
      verify(_mockFileClient).openInputStream();
      verify(_mockFileOpenInputStreamResult).getInputStream();

      // Verify file was created
      assertTrue(tempFile.exists());

      // Verify content was written correctly
      byte[] writtenContent = Files.readAllBytes(tempFile.toPath());
      assertArrayEquals(testData, writtenContent);
    } finally {
      // Cleanup
      FileUtils.deleteQuietly(tempFile);
    }
  }

  @Test
  public void testCopyToLocalFileExistingDirectory() throws Exception {
    // Create a temporary directory for the test
    File tempDir = new File(System.getProperty("java.io.tmpdir"), "existing_dir");
    tempDir.mkdirs();

    // Mock file stream
    byte[] testData = "test data".getBytes();
    InputStream mockInputStream = new ByteArrayInputStream(testData);
    when(_mockFileSystemClient.getFileClient(any())).thenReturn(_mockFileClient);
    when(_mockFileClient.openInputStream()).thenReturn(_mockFileOpenInputStreamResult);
    when(_mockFileOpenInputStreamResult.getInputStream()).thenReturn(mockInputStream);

    try {
      // Execute
      _adlsGen2PinotFsUnderTest.copyToLocalFile(_mockURI, tempDir);

      // Verify file operations in order
      verify(_mockFileSystemClient).getFileClient(AzurePinotFSUtil.convertUriToAzureStylePath(_mockURI));
      verify(_mockFileClient).openInputStream();
      verify(_mockFileOpenInputStreamResult).getInputStream();

      // Verify directory was overwritten with file
      assertTrue(tempDir.exists());
      assertFalse(tempDir.isDirectory());

      // Verify content was written correctly
      byte[] writtenContent = Files.readAllBytes(tempDir.toPath());
      assertArrayEquals(testData, writtenContent);
    } finally {
      // Cleanup
      if (tempDir.exists()) {
        FileUtils.deleteQuietly(tempDir);
      }
    }
  }

  @Test
  public void testCopyFromLocalFileViaBlobApi() throws Exception {
    // Create a test instance with Blob API support
    ADLSGen2PinotFS blobEnabledFs = new ADLSGen2PinotFS(_mockFileSystemClient, _mockBlobContainerClient);

    // Create a temporary file with test data
    File tempFile = File.createTempFile("pinot_blob_test", ".tmp");
    byte[] testData = "test segment data".getBytes(StandardCharsets.UTF_8);
    Files.write(tempFile.toPath(), testData);

    URI dstUri = new URI("adl2://account/container/test_segment");
    String expectedPath = AzurePinotFSUtil.convertUriToAzureStylePath(dstUri);

    when(_mockBlobContainerClient.getBlobClient(expectedPath)).thenReturn(_mockBlobClient);
    when(_mockBlobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), any(Context.class)))
        .thenReturn(null);

    try {
      blobEnabledFs.copyFromLocalFile(tempFile, dstUri);

      verify(_mockBlobContainerClient).getBlobClient(expectedPath);
      verify(_mockBlobClient).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), any(Context.class));
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }

  @Test
  public void testCopyFromLocalFileViaBlobApiWithChecksumEnabled() throws Exception {
    // Create a test instance with Blob API support and checksum enabled
    ADLSGen2PinotFS blobEnabledFs = new ADLSGen2PinotFS(_mockFileSystemClient, _mockBlobContainerClient, true);

    // Create a temporary file with test data
    File tempFile = File.createTempFile("pinot_blob_checksum_test", ".tmp");
    byte[] testData = "test segment data".getBytes(StandardCharsets.UTF_8);
    Files.write(tempFile.toPath(), testData);

    URI dstUri = new URI("adl2://account/container/test_segment");
    String expectedPath = AzurePinotFSUtil.convertUriToAzureStylePath(dstUri);
    String expectedBlockId =
        Base64.getEncoder().encodeToString(String.format("%08d", 0).getBytes(StandardCharsets.UTF_8));

    BlockBlobClient mockBlockBlobClient = mock(BlockBlobClient.class);
    when(_mockBlobContainerClient.getBlobClient(expectedPath)).thenReturn(_mockBlobClient);
    when(_mockBlobClient.getBlockBlobClient()).thenReturn(mockBlockBlobClient);

    ArgumentCaptor<BlobHttpHeaders> headersCaptor = ArgumentCaptor.forClass(BlobHttpHeaders.class);

    try {
      blobEnabledFs.copyFromLocalFile(tempFile, dstUri);

      verify(_mockBlobContainerClient).getBlobClient(expectedPath);
      verify(_mockBlobClient).getBlockBlobClient();
      verify(_mockBlobClient, never()).getProperties();
      verify(_mockBlobClient, never()).upload(any(InputStream.class), anyLong(), anyBoolean());
      verify(_mockBlobClient, never()).setHttpHeaders(any(BlobHttpHeaders.class));

      verify(mockBlockBlobClient).stageBlockWithResponse(eq(expectedBlockId), any(InputStream.class),
          eq((long) testData.length), any(byte[].class), isNull(), isNull(), eq(Context.NONE));
      verify(mockBlockBlobClient).commitBlockListWithResponse(eq(List.of(expectedBlockId)), headersCaptor.capture(),
          isNull(), isNull(), isNull(), isNull(), eq(Context.NONE));

      byte[] expectedContentMd5 = MessageDigest.getInstance("MD5").digest(testData);
      BlobHttpHeaders uploadedHeaders = headersCaptor.getValue();
      assertArrayEquals(uploadedHeaders.getContentMd5(), expectedContentMd5);
      verifyNoMoreInteractions(mockBlockBlobClient);
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }

  @Test
  public void testCopyDirViaBlobApiWithChecksumEnabledAndNoContentMd5() throws Exception {
    DataLakeFileSystemClient mockFileSystemClient = mock(DataLakeFileSystemClient.class);
    BlobContainerClient mockBlobContainerClient = mock(BlobContainerClient.class);
    BlobClient mockBlobClient = mock(BlobClient.class);
    BlockBlobClient mockBlockBlobClient = mock(BlockBlobClient.class);
    DataLakeDirectoryClient mockSrcDirectoryClient = mock(DataLakeDirectoryClient.class);
    DataLakeDirectoryClient mockDstDirectoryClient = mock(DataLakeDirectoryClient.class);
    DataLakeStorageException mockNotFoundException = mock(DataLakeStorageException.class);
    PathProperties mockSrcDirectoryPathProperties = mock(PathProperties.class);
    DataLakeFileClient mockSrcFileClient = mock(DataLakeFileClient.class);
    PathProperties mockSrcFilePathProperties = mock(PathProperties.class);
    DataLakeFileOpenInputStreamResult mockOpenInputStreamResult = mock(DataLakeFileOpenInputStreamResult.class);

    ADLSGen2PinotFS blobEnabledFs = new ADLSGen2PinotFS(mockFileSystemClient, mockBlobContainerClient, true);

    URI srcUri = new URI("adl2://account/container/src");
    URI dstUri = new URI("adl2://account/container/dst");
    String srcPath = AzurePinotFSUtil.convertUriToAzureStylePath(srcUri);
    String dstPath = AzurePinotFSUtil.convertUriToAzureStylePath(dstUri);

    byte[] testData = "test segment data".getBytes(StandardCharsets.UTF_8);
    String expectedBlockId =
        Base64.getEncoder().encodeToString(String.format("%08d", 0).getBytes(StandardCharsets.UTF_8));

    when(mockFileSystemClient.getDirectoryClient(dstPath)).thenReturn(mockDstDirectoryClient);
    when(mockDstDirectoryClient.getProperties()).thenThrow(mockNotFoundException);
    when(mockNotFoundException.getStatusCode()).thenReturn(404);

    when(mockFileSystemClient.getDirectoryClient(srcPath)).thenReturn(mockSrcDirectoryClient);
    when(mockSrcDirectoryClient.getProperties()).thenReturn(mockSrcDirectoryPathProperties);
    HashMap<String, String> srcMetadata = new HashMap<>();
    srcMetadata.put("hdi_isfolder", "false");
    when(mockSrcDirectoryPathProperties.getMetadata()).thenReturn(srcMetadata);

    when(mockFileSystemClient.getFileClient(srcPath)).thenReturn(mockSrcFileClient);
    when(mockSrcFileClient.getProperties()).thenReturn(mockSrcFilePathProperties);
    when(mockSrcFilePathProperties.getContentMd5()).thenReturn(null);
    when(mockSrcFilePathProperties.getFileSize()).thenReturn((long) testData.length);
    when(mockSrcFileClient.openInputStream()).thenReturn(mockOpenInputStreamResult);
    when(mockOpenInputStreamResult.getInputStream()).thenReturn(new ByteArrayInputStream(testData));

    when(mockBlobContainerClient.getBlobClient(dstPath)).thenReturn(mockBlobClient);
    when(mockBlobClient.getBlockBlobClient()).thenReturn(mockBlockBlobClient);

    assertTrue(blobEnabledFs.copyDir(srcUri, dstUri));

    verify(mockFileSystemClient).getDirectoryClient(dstPath);
    verify(mockDstDirectoryClient).getProperties();
    verify(mockNotFoundException).getStatusCode();
    verify(mockFileSystemClient).getDirectoryClient(srcPath);
    verify(mockSrcDirectoryClient).getProperties();
    verify(mockSrcDirectoryPathProperties).getMetadata();
    verify(mockFileSystemClient, times(2)).getFileClient(srcPath);
    verify(mockSrcFileClient).getProperties();
    verify(mockSrcFilePathProperties).getContentMd5();
    verify(mockSrcFilePathProperties).getFileSize();
    verify(mockSrcFileClient).openInputStream();
    verify(mockOpenInputStreamResult).getInputStream();

    verify(mockBlobContainerClient).getBlobClient(dstPath);
    verify(mockBlobClient).getBlockBlobClient();
    verify(mockBlobClient, never()).getProperties();
    verify(mockBlobClient, never()).upload(any(InputStream.class), anyLong(), anyBoolean());
    verify(mockBlobClient, never()).setHttpHeaders(any(BlobHttpHeaders.class));

    verify(mockBlockBlobClient).stageBlockWithResponse(eq(expectedBlockId), any(InputStream.class),
        eq((long) testData.length), any(byte[].class), isNull(), isNull(), eq(Context.NONE));
    verify(mockBlockBlobClient).commitBlockList(eq(List.of(expectedBlockId)));
    verify(mockBlockBlobClient, never()).commitBlockListWithResponse(anyList(), any(BlobHttpHeaders.class), any(),
        any(), any(), any(), any());

    verifyNoMoreInteractions(mockFileSystemClient, mockBlobContainerClient, mockBlobClient, mockBlockBlobClient,
        mockSrcDirectoryClient, mockDstDirectoryClient, mockNotFoundException, mockSrcDirectoryPathProperties,
        mockSrcFileClient, mockSrcFilePathProperties, mockOpenInputStreamResult);
  }

  @Test
  public void testCopyFromLocalFileViaBlobApiWithException() throws Exception {
    // Create a test instance with Blob API support
    ADLSGen2PinotFS blobEnabledFs = new ADLSGen2PinotFS(_mockFileSystemClient, _mockBlobContainerClient);

    // Create a temporary file with test data
    File tempFile = File.createTempFile("pinot_blob_test", ".tmp");
    byte[] testData = "test segment data".getBytes(StandardCharsets.UTF_8);
    Files.write(tempFile.toPath(), testData);

    URI dstUri = new URI("adl2://account/container/test_segment");
    String expectedPath = AzurePinotFSUtil.convertUriToAzureStylePath(dstUri);

    BlobStorageException blobException = mock(BlobStorageException.class);
    when(blobException.getStatusCode()).thenReturn(500);
    when(_mockBlobContainerClient.getBlobClient(expectedPath)).thenReturn(_mockBlobClient);
    doThrow(blobException).when(_mockBlobClient)
        .uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), any(Context.class));

    try {
      expectThrows(IOException.class, () -> blobEnabledFs.copyFromLocalFile(tempFile, dstUri));

      verify(_mockBlobContainerClient).getBlobClient(expectedPath);
      verify(_mockBlobClient).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), any(Context.class));
      verify(blobException).getStatusCode();
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }
}
