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
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS;
import org.apache.pinot.plugin.filesystem.AzurePinotFSUtil;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


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
        _mockFileClient, _mockFileOpenInputStreamResult, _mockInputStream);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testInitNoAuth() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    _adlsGen2PinotFsUnderTest.init(pinotConfiguration);
  }

  @Test
  public void testGetOrCreateClientWithFileSystemGet() {
    when(_mockServiceClient.getFileSystemClient(MOCK_FILE_SYSTEM_NAME)).thenReturn(_mockFileSystemClient);
    when(_mockFileSystemClient.getProperties()).thenReturn(null);

    final DataLakeFileSystemClient actual =
        _adlsGen2PinotFsUnderTest.getOrCreateClientWithFileSystem(_mockServiceClient, MOCK_FILE_SYSTEM_NAME);
    Assert.assertEquals(actual, _mockFileSystemClient);

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
    Assert.assertEquals(actual, _mockFileSystemClient);

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
    Assert.assertTrue(actual);

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
    Assert.assertTrue(actual);

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
    Assert.assertTrue(actual);

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
    Assert.assertEquals(actual[0], "/foo");

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
    Assert.assertEquals(fm.getFilePath(), "/foo");
    Assert.assertFalse(fm.isDirectory());
    Assert.assertEquals(fm.getLength(), 1024);
    Assert.assertEquals(fm.getLastModifiedTime(), mtime.toInstant().toEpochMilli());

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
    Assert.assertEquals(actual, now.toEpochMilli());

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getLastModified();
  }

  @Test
  public void testListFilesException() {
    when(_mockFileSystemClient.listPaths(any(), any())).thenThrow(_mockDataLakeStorageException);

    Assert.expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.listFiles(_mockURI, true));

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
    Assert.assertTrue(actual);

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
    Assert.assertTrue(actual);

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
    Assert.assertTrue(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).rename(eq(null), any());
  }

  @Test
  public void testDoMoveException() {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenThrow(_mockDataLakeStorageException);

    Assert.expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.doMove(_mockURI, _mockURI));

    verify(_mockFileSystemClient).getDirectoryClient(any());
  }

  @Test
  public void testExistsTrue()
      throws IOException {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenReturn(_mockPathProperties);

    boolean actual = _adlsGen2PinotFsUnderTest.exists(_mockURI);
    Assert.assertTrue(actual);

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
    Assert.assertFalse(actual);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockDataLakeStorageException).getStatusCode();
  }

  @Test
  public void testExistsException() {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenThrow(_mockDataLakeStorageException);
    when(_mockDataLakeStorageException.getStatusCode()).thenReturn(123);

    Assert.expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.exists(_mockURI));

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
    Assert.assertEquals(actual, testLength);

    verify(_mockFileSystemClient).getDirectoryClient(any());
    verify(_mockDirectoryClient).getProperties();
    verify(_mockPathProperties).getFileSize();
  }

  @Test
  public void testLengthException() {
    when(_mockFileSystemClient.getDirectoryClient(any())).thenReturn(_mockDirectoryClient);
    when(_mockDirectoryClient.getProperties()).thenThrow(_mockDataLakeStorageException);

    Assert.expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.length(_mockURI));

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
    Assert.assertTrue(actual);

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

    Assert.expectThrows(IOException.class, () -> _adlsGen2PinotFsUnderTest.touch(_mockURI));

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
    Assert.assertEquals(actual, _mockInputStream);

    verify(_mockFileSystemClient).getFileClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(_mockURI));
    verify(_mockFileClient).openInputStream();
    verify(_mockFileOpenInputStreamResult).getInputStream();
  }
}
