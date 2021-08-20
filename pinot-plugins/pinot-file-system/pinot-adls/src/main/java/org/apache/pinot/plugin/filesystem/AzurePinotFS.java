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
package org.apache.pinot.plugin.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Azure implementation for the PinotFS interface. This class will implement using azure-data-lake libraries all
 * the basic FS methods needed within Pinot.
 */
public class AzurePinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzurePinotFS.class);
  private static final int BUFFER_SIZE = 4096;
  private ADLStoreClient _adlStoreClient;
  private static final String[] EMPTY_ARR = new String[0];
  public static final String ACCOUNT_ID = "accountId";
  public static final String AUTH_ENDPOINT = "authEndpoint";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";

  public AzurePinotFS() {

  }

  @VisibleForTesting
  public AzurePinotFS(ADLStoreClient adlStoreClient) {
    _adlStoreClient = adlStoreClient;
  }

  @Override
  public void init(PinotConfiguration config) {
    // The ADL account id. Example: {@code mystore.azuredatalakestore.net}.
    String account = config.getProperty(ACCOUNT_ID);
    // The endpoint that should be used for authentication.
    // Usually of the form {@code https://login.microsoftonline.com/<tenant-id>/oauth2/token}.
    String authEndpoint = config.getProperty(AUTH_ENDPOINT);
    // The clientId used to authenticate this application
    String clientId = config.getProperty(CLIENT_ID);
    // The secret key used to authenticate this application
    String clientSecret = config.getProperty(CLIENT_SECRET);

    AccessTokenProvider tokenProvider = new ClientCredsTokenProvider(authEndpoint, clientId, clientSecret);
    _adlStoreClient = ADLStoreClient.createClient(account, tokenProvider);
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    return _adlStoreClient.createDirectory(uri.getPath());
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    // Returns false if directory we want to delete is not empty
    if (isDirectory(segmentUri) && listFiles(segmentUri, false).length > 0 && !forceDelete) {
      return false;
    }
    return _adlStoreClient.deleteRecursive(segmentUri.getPath());
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    return _adlStoreClient.rename(srcUri.getPath(), dstUri.getPath());
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri)
      throws IOException {
    if (exists(dstUri)) {
      delete(dstUri, true);
    }

    _adlStoreClient.createEmptyFile(dstUri.getPath());
    try {
      InputStream inputStream = _adlStoreClient.getReadStream(srcUri.getPath());
      OutputStream outputStream = _adlStoreClient.getAppendStream(dstUri.getPath());

      int bytesRead;
      byte[] buffer = new byte[BUFFER_SIZE];
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
      inputStream.close();
      outputStream.close();
    } catch (IOException e) {
      LOGGER.error("Exception encountered during copy, input: '{}', output: '{}'.", srcUri.toString(), dstUri.toString(), e);
    }
    return true;
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    return _adlStoreClient.checkExists(fileUri.getPath());
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    // get file metadata
    DirectoryEntry ent = _adlStoreClient.getDirectoryEntry(fileUri.getPath());
    return ent.length;
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    DirectoryEntry rootDir = _adlStoreClient.getDirectoryEntry(fileUri.getPath());
    if (rootDir == null) {
      return EMPTY_ARR;
    }

    if (!recursive) {
      List<DirectoryEntry> shallowDirectoryEntries = _adlStoreClient.enumerateDirectory(rootDir.fullName);
      List<String> shallowDirPaths = new ArrayList<>(shallowDirectoryEntries.size());
      for (DirectoryEntry directoryEntry : shallowDirectoryEntries) {
        shallowDirPaths.add(directoryEntry.fullName);
      }
      return shallowDirPaths.toArray(new String[shallowDirPaths.size()]);
    }

    List<DirectoryEntry> directoryEntries = listFiles(rootDir);
    List<String> fullFilePaths = new ArrayList<>(directoryEntries.size());
    for (DirectoryEntry directoryEntry : directoryEntries) {
      fullFilePaths.add(directoryEntry.fullName);
    }
    return fullFilePaths.toArray(new String[fullFilePaths.size()]);
  }

  private List<DirectoryEntry> listFiles(DirectoryEntry origDirEntry)
      throws IOException {
    List<DirectoryEntry> fileList = new ArrayList<>();
    if (origDirEntry.type.equals(DirectoryEntryType.DIRECTORY)) {
      for (DirectoryEntry directoryEntry : _adlStoreClient.enumerateDirectory(origDirEntry.fullName)) {
        fileList.add(directoryEntry);
        fileList.addAll(listFiles(directoryEntry));
      }
    } else {
      fileList.add(origDirEntry);
    }
    return fileList;
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    if (dstFile.exists()) {
      if (dstFile.isDirectory()) {
        FileUtils.deleteDirectory(dstFile);
      } else {
        FileUtils.deleteQuietly(dstFile);
      }
    }
    try (InputStream adlStream = _adlStoreClient.getReadStream(srcUri.getPath())) {
      Path dstFilePath = Paths.get(dstFile.toURI());

      /* Copy the source file to the destination directory as a file with the same name as the source,
       * replace an existing file with the same name in the destination directory, if any.
       * Set new file permissions on the copied file.
       */
      Files.copy(adlStream, dstFilePath);
    } catch (Exception ex) {
      throw ex;
    }
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    OutputStream stream = _adlStoreClient.createFile(dstUri.getPath(), IfExists.OVERWRITE);
    PrintStream out = new PrintStream(stream);
    byte[] inputStream = IOUtils.toByteArray(new FileInputStream(srcFile));
    out.write(inputStream);
    out.close();
  }

  @Override
  public boolean isDirectory(URI uri) {
    DirectoryEntry dirEntry;
    try {
      dirEntry = _adlStoreClient.getDirectoryEntry(uri.getPath());
    } catch (IOException e) {
      LOGGER.error("Could not get directory entry for {}", uri);
      throw new RuntimeException(e);
    }

    return dirEntry.type.equals(DirectoryEntryType.DIRECTORY);
  }

  @Override
  public long lastModified(URI uri) {
    try {
      return _adlStoreClient.getDirectoryEntry(uri.getPath()).lastModifiedTime.getTime();
    } catch (IOException e) {
      LOGGER.error("Could not get directory entry for {}", uri);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    if (!exists(uri)) {
      _adlStoreClient.createEmptyFile(uri.getPath());
    } else {
      _adlStoreClient.setTimes(uri.getPath(), null, new Date());
    }
    return true;
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    return _adlStoreClient.getReadStream(uri.getPath());
  }
}
