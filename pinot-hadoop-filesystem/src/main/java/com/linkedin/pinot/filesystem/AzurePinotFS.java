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
package com.linkedin.pinot.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Azure implementation for the PinotFS interface. This class will implement using azure-data-lake libraries all
 * the basic FS methods needed within Pinot.
 */
public class AzurePinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzurePinotFS.class);
  private ADLStoreClient _adlStoreClient;

  private static final String AZURE_KEY = "azure";
  private static final String ACCOUNT_KEY = "account";
  private static final String AUTH_ENDPOINT_KEY = "authendpoint";
  private static final String CLIENT_ID_KEY = "clientid";
  private static final String CLIENT_SECRET_KEY = "clientsecret";

  public AzurePinotFS() {

  }

  @VisibleForTesting
  public AzurePinotFS(ADLStoreClient adlStoreClient) {
    _adlStoreClient = adlStoreClient;
  }

  @Override
  public void init(Configuration config) {
    Configuration azureConfigs = config.subset(AZURE_KEY);
    // The ADL account id. Example: {@code mystore.azuredatalakestore.net}.
    String account = azureConfigs.getString(ACCOUNT_KEY);
    // The endpoint that should be used for authentication.
    // Usually of the form {@code https://login.microsoftonline.com/<tenant-id>/oauth2/token}.
    String authEndpoint = azureConfigs.getString(AUTH_ENDPOINT_KEY);
    // The clientId used to authenticate this application
    String clientId = azureConfigs.getString(CLIENT_ID_KEY);
    // The secret key used to authenticate this application
    String clientSecret = azureConfigs.getString(CLIENT_SECRET_KEY);

    AccessTokenProvider tokenProvider =
        new ClientCredsTokenProvider(authEndpoint, clientId, clientSecret);
    _adlStoreClient = ADLStoreClient.createClient(account, tokenProvider);
  }

  @Override
  public boolean mkdir(URI uri) throws IOException {
    return _adlStoreClient.createDirectory(uri.getPath());
  }

  @Override
  public boolean delete(URI segmentUri) throws IOException {
    return _adlStoreClient.deleteRecursive(segmentUri.getPath());
  }

  @Override
  public boolean move(URI srcUri, URI dstUri) throws IOException {
    //rename the file
    return _adlStoreClient.rename(srcUri.getPath(), dstUri.getPath());
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    if (exists(dstUri)) {
      delete(dstUri);
    }
    _adlStoreClient.createEmptyFile(dstUri.getPath());
    ADLFileOutputStream appendStream = _adlStoreClient.getAppendStream(dstUri.getPath());
    ADLFileInputStream readStream = _adlStoreClient.getReadStream(srcUri.getPath());
    appendStream.write(readStream.read());
    appendStream.close();
    readStream.close();
    return true;
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    return _adlStoreClient.checkExists(fileUri.getPath());
  }

  @Override
  public long length(URI fileUri) throws IOException {
    // get file metadata
    DirectoryEntry ent = _adlStoreClient.getDirectoryEntry(fileUri.getPath());
    return ent.length;
  }

  @Override
  public String[] listFiles(URI fileUri) throws IOException {
    // list directory contents
    List<DirectoryEntry> list = _adlStoreClient.enumerateDirectory(fileUri.getPath(), Integer.MAX_VALUE);
    List<String> fileNames = new ArrayList<>();
    for (DirectoryEntry directoryEntry : list) {
      fileNames.add(directoryEntry.name);
    }
    return fileNames.toArray(new String[fileNames.size()]);
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
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

      LOGGER.info("Copied file {} from ADL to {}", srcUri, dstFile);
    } catch (Exception ex) {
      throw ex;
    }
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws IOException {
    // TODO: Add this method. Not needed for now because of metadata upload
    throw new UnsupportedOperationException("Cannot copy from local to Azure");
  }
}
