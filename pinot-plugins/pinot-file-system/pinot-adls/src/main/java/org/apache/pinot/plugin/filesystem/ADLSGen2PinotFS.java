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

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.Utility;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Azure Data Lake Storage Gen2 implementation for the PinotFS interface.
 */
public class ADLSGen2PinotFS extends BasePinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(ADLSGen2PinotFS.class);

  private enum AuthenticationType {
    ACCESS_KEY, AZURE_AD, AZURE_AD_WITH_PROXY, ANONYMOUS_ACCESS, DEFAULT
  }

  private static final String AUTHENTICATION_TYPE = "authenticationType";
  private static final String ACCOUNT_NAME = "accountName";
  private static final String ACCESS_KEY = "accessKey";
  private static final String FILE_SYSTEM_NAME = "fileSystemName";
  private static final String ENABLE_CHECKSUM = "enableChecksum";
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String TENANT_ID = "tenantId";
  private static final String MANAGED_IDENTITY_CLIENT_ID = "managedIdentityClientId";
  private static final String AUTHORITY_HOST = "authorityHost";
  private static final String PROXY_HOST = "proxyHost";
  private static final String PROXY_PORT = "proxyPort";
  private static final String PROXY_USERNAME = "proxyUsername";
  private static final String PROXY_PASSWORD = "proxyPassword";

  private static final String HTTPS_URL_PREFIX = "https://";

  private static final String AZURE_STORAGE_DNS_SUFFIX = ".dfs.core.windows.net";
  private static final String AZURE_BLOB_DNS_SUFFIX = ".blob.core.windows.net";
  private static final String PATH_ALREADY_EXISTS_ERROR_CODE = "PathAlreadyExists";
  private static final String CONTAINER_NOT_FOUND_ERROR_CODE = "ContainerNotFound";
  private static final String IS_DIRECTORY_KEY = "hdi_isfolder";

  private static final int NOT_FOUND_STATUS_CODE = 404;
  private static final int ALREADY_EXISTS_STATUS_CODE = 409;

  // Azure Data Lake Gen2's block size is 4MB
  private static final int BUFFER_SIZE = 4 * 1024 * 1024;

  private DataLakeFileSystemClient _fileSystemClient;

  // If enabled, pinotFS implementation will guarantee that the bits you've read are the same as the ones you wrote.
  // However, there's some overhead in computing hash. (Adds roughly 3 seconds for 1GB file)
  private boolean _enableChecksum;

  public ADLSGen2PinotFS() {
  }

  public ADLSGen2PinotFS(DataLakeFileSystemClient fileSystemClient) {
    _fileSystemClient = fileSystemClient;
  }

  @Override
  public void init(PinotConfiguration config) {
    _enableChecksum = config.getProperty(ENABLE_CHECKSUM, false);

    // Azure storage account name
    String accountName = config.getProperty(ACCOUNT_NAME);

    // TODO: consider to add the encryption of the following config
    String authTypeStr = config.getProperty(AUTHENTICATION_TYPE, AuthenticationType.ACCESS_KEY.name());
    AuthenticationType authType = AuthenticationType.valueOf(authTypeStr.toUpperCase());
    String accessKey = config.getProperty(ACCESS_KEY);
    String fileSystemName = config.getProperty(FILE_SYSTEM_NAME);
    String clientId = config.getProperty(CLIENT_ID);
    String clientSecret = config.getProperty(CLIENT_SECRET);
    String tenantId = config.getProperty(TENANT_ID);
    String managedIdentityClientId = config.getProperty(MANAGED_IDENTITY_CLIENT_ID);
    String authorityHost = config.getProperty(AUTHORITY_HOST);
    String proxyHost = config.getProperty(PROXY_HOST);
    String proxyUsername = config.getProperty(PROXY_USERNAME);
    String proxyPassword = config.getProperty(PROXY_PASSWORD);
    String proxyPort = config.getProperty(PROXY_PORT);

    String dfsServiceEndpointUrl = HTTPS_URL_PREFIX + accountName + AZURE_STORAGE_DNS_SUFFIX;

    DataLakeServiceClientBuilder dataLakeServiceClientBuilder =
        new DataLakeServiceClientBuilder().endpoint(dfsServiceEndpointUrl);

    switch (authType) {
      case ACCESS_KEY: {
        LOGGER.info("Authenticating using the access key to the account.");
        Preconditions.checkNotNull(accountName, "Account Name cannot be null");
        Preconditions.checkNotNull(accessKey, "Access Key cannot be null");

        StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accessKey);
        dataLakeServiceClientBuilder.credential(sharedKeyCredential);
        break;
      }
      case AZURE_AD: {
        LOGGER.info("Authenticating using Azure Active Directory");
        Preconditions.checkNotNull(clientId, "Client ID cannot be null");
        Preconditions.checkNotNull(clientSecret, "ClientSecret cannot be null");
        Preconditions.checkNotNull(tenantId, "TenantId cannot be null");

        ClientSecretCredential clientSecretCredential =
            new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).tenantId(tenantId)
                .build();
        dataLakeServiceClientBuilder.credential(clientSecretCredential);
        break;
      }
      case AZURE_AD_WITH_PROXY: {
        LOGGER.info("Authenticating using Azure Active Directory with proxy");
        Preconditions.checkNotNull(clientId, "Client Id cannot be null");
        Preconditions.checkNotNull(clientSecret, "ClientSecret cannot be null");
        Preconditions.checkNotNull(tenantId, "Tenant Id cannot be null");
        Preconditions.checkNotNull(proxyHost, "Proxy Host cannot be null");
        Preconditions.checkNotNull(proxyPort, "Proxy Port cannot be null");
        Preconditions.checkNotNull(proxyUsername, "Proxy Username cannot be null");
        Preconditions.checkNotNull(proxyPassword, "Proxy Password cannot be null");

        NettyAsyncHttpClientBuilder builder = new NettyAsyncHttpClientBuilder();
        builder.proxy(
            new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)))
                .setCredentials(proxyUsername, proxyPassword));
        ClientSecretCredentialBuilder clientSecretCredentialBuilder =
            new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).tenantId(tenantId);
        clientSecretCredentialBuilder.httpClient(builder.build());

        dataLakeServiceClientBuilder.credential(clientSecretCredentialBuilder.build());
        break;
      }
      case DEFAULT: {
        LOGGER.info("Authenticating using Azure default credential");
        DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder();
        if (tenantId != null) {
          LOGGER.info("Set tenant ID to {}", tenantId);
          defaultAzureCredentialBuilder.tenantId(tenantId);
        }
        if (managedIdentityClientId != null) {
          LOGGER.info("Set managed identity client ID to {}", managedIdentityClientId);
          defaultAzureCredentialBuilder.managedIdentityClientId(managedIdentityClientId);
        }
        if (authorityHost != null) {
          LOGGER.info("Set authority host to {}", authorityHost);
          defaultAzureCredentialBuilder.authorityHost(authorityHost);
        }
        dataLakeServiceClientBuilder.credential(defaultAzureCredentialBuilder.build());
        break;
      }
      case ANONYMOUS_ACCESS: {
        LOGGER.info("Authenticating using anonymous access");
        break;
      }
      default: {
        // Should never reach here
        throw new IllegalStateException("Unexpected authType: " + authType);
      }
    }

    DataLakeServiceClient serviceClient = dataLakeServiceClientBuilder.buildClient();
    _fileSystemClient = getOrCreateClientWithFileSystem(serviceClient, fileSystemName);

    LOGGER.info("ADLSGen2PinotFS is initialized (accountName={}, fileSystemName={}, dfsServiceEndpointUrl={}, "
        + "enableChecksum={})", accountName, fileSystemName, dfsServiceEndpointUrl, _enableChecksum);
  }

  /**
   * Returns the DataLakeFileSystemClient to the specified file system creating if it doesn't exist.
   *
   * @param serviceClient authenticated data lake service client to an account
   * @param fileSystemName name of the file system (blob container)
   * @return DataLakeFileSystemClient with the specified fileSystemName.
   */
  @VisibleForTesting
  public DataLakeFileSystemClient getOrCreateClientWithFileSystem(DataLakeServiceClient serviceClient,
      String fileSystemName) {
    try {
      DataLakeFileSystemClient fileSystemClient = serviceClient.getFileSystemClient(fileSystemName);
      // The return value is irrelevant. This is to test if the filesystem exists.
      fileSystemClient.getProperties();
      return fileSystemClient;
    } catch (DataLakeStorageException e) {
      if (e.getStatusCode() == NOT_FOUND_STATUS_CODE && e.getErrorCode().equals(CONTAINER_NOT_FOUND_ERROR_CODE)) {
        LOGGER.info("FileSystem with name {} does not exist. Creating one with the same name.", fileSystemName);
        return serviceClient.createFileSystem(fileSystemName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Make a new directory at the given location.
   *
   * @param uri location to make the directory.
   * @return true if creation succeeds else false.
   */
  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    LOGGER.debug("mkdir is called with uri='{}'", uri);
    try {
      // By default, create directory call will overwrite if the path already exists. Setting IfNoneMatch = "*" to
      // prevent overwrite. https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
      DataLakeRequestConditions requestConditions = new DataLakeRequestConditions().setIfNoneMatch("*");
      _fileSystemClient
          .createDirectoryWithResponse(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri), null, null, null,
              null, requestConditions, null, null);
      return true;
    } catch (DataLakeStorageException e) {
      // If the path already exists, doing nothing and return true
      if (e.getStatusCode() == ALREADY_EXISTS_STATUS_CODE && e.getErrorCode().equals(PATH_ALREADY_EXISTS_ERROR_CODE)) {
        return true;
      }
      LOGGER.error("Exception thrown while calling mkdir (uri={}, errorStatus ={})", uri, e.getStatusCode(), e);
      throw new IOException(e);
    }
  }

  /**
   * Deletes a file/directory at a given location.
   *
   * @param segmentUri location to delete
   * @param forceDelete to force delete non empty directory.
   * @return true if deletion succeeds else false.
   */
  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    LOGGER.debug("delete is called with segmentUri='{}', forceDelete='{}'", segmentUri, forceDelete);
    try {
      boolean isDirectory = isDirectory(segmentUri);
      if (isDirectory && listFiles(segmentUri, false).length > 0 && !forceDelete) {
        return false;
      }

      String path = AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(segmentUri);
      if (isDirectory) {
        _fileSystemClient.deleteDirectoryWithResponse(path, true, null, null, Context.NONE).getValue();
      } else {
        _fileSystemClient.deleteFile(path);
      }
      return true;
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  /**
   * Move a file from source location to destination location.
   *
   * @param srcUri location to move the file from
   * @param dstUri location to move the file to
   * @return true if move succeeds else false.
   */
  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    LOGGER.debug("doMove is called with srcUri='{}', dstUri='{}'", srcUri, dstUri);
    try {
      DataLakeDirectoryClient directoryClient =
          _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(srcUri));
      directoryClient.rename(null, AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(dstUri));
      return true;
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  /**
   * Copy a file from source location to destination location.
   *
   * @param srcUri location to copy the file from
   * @param dstUri location to copy the file to
   * @return true if move succeeds else false.
   */
  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
      throws IOException {
    LOGGER.debug("copy is called with srcUri='{}', dstUri='{}'", srcUri, dstUri);
    // If src and dst are the same, do nothing.
    if (srcUri.equals(dstUri)) {
      return true;
    }

    // Remove the destination directory or file
    if (exists(dstUri)) {
      delete(dstUri, true);
    }

    if (!isDirectory(srcUri)) {
      // If source is a file, we can simply copy the file from src to dst
      return copySrcToDst(srcUri, dstUri);
    } else {
      // In case we are copying a directory, we need to recursively look into the directory and copy all the files and
      // directories accordingly
      try {
        boolean copySucceeded = true;
        Path srcPath = Paths.get(srcUri.getPath());
        for (String path : listFiles(srcUri, true)) {
          // Compute the src path for the given path
          URI currentSrc = new URI(srcUri.getScheme(), srcUri.getHost(), path, null);

          // Compute the destination path for the current path.
          String relativeSrcPath = srcPath.relativize(Paths.get(path)).toString();
          String newDstPath = Paths.get(dstUri.getPath(), relativeSrcPath).toString();
          URI newDst = new URI(dstUri.getScheme(), dstUri.getHost(), newDstPath, null);

          if (isDirectory(currentSrc)) {
            // If src is directory, create one.
            mkdir(newDst);
          } else {
            // If src is a file, we need to copy.
            copySucceeded &= copySrcToDst(currentSrc, newDst);
          }
        }
        return copySucceeded;
      } catch (DataLakeStorageException | URISyntaxException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Checks if the file exists at a given location
   *
   * @param fileUri location to check the existance of the file.
   * @return true if exists else false.
   */
  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    try {
      _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(fileUri))
          .getProperties();
      return true;
    } catch (DataLakeStorageException e) {
      if (e.getStatusCode() == NOT_FOUND_STATUS_CODE) {
        return false;
      }
      throw new IOException(e);
    }
  }

  /**
   * Find the size of the file.
   *
   * @param fileUri location of the file to find the size of.
   * @return size of the file
   */
  @Override
  public long length(URI fileUri)
      throws IOException {
    try {
      PathProperties pathProperties =
          _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(fileUri))
              .getProperties();
      return pathProperties.getFileSize();
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  /**
   * List the names of files in a given directory.
   *
   * @param fileUri location to move the file from
   * @param recursive flag to check the sub directories.
   * @return array of all the files in the target directory.
   */
  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    LOGGER.debug("listFiles is called with fileUri='{}', recursive='{}'", fileUri, recursive);
    try {
      PagedIterable<PathItem> iter = listPathItems(fileUri, recursive);
      return iter.stream().map(p -> AzurePinotFSUtil.convertAzureStylePathToUriStylePath(p.getName()))
          .toArray(String[]::new);
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
      throws IOException {
    LOGGER.debug("listFilesWithMetadata is called with fileUri='{}', recursive='{}'", fileUri, recursive);
    try {
      PagedIterable<PathItem> iter = listPathItems(fileUri, recursive);
      return iter.stream().map(ADLSGen2PinotFS::getFileMetadata).collect(Collectors.toList());
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  private PagedIterable<PathItem> listPathItems(URI fileUri, boolean recursive)
      throws IOException {
    // Unlike other Azure SDK APIs that takes url encoded path, ListPathsOptions takes decoded url
    // e.g) 'path/segment' instead of 'path%2Fsegment'
    String pathForListPathsOptions = Utility.urlDecode(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(fileUri));
    ListPathsOptions options = new ListPathsOptions().setPath(pathForListPathsOptions).setRecursive(recursive);
    return _fileSystemClient.listPaths(options, null);
  }

  private static FileMetadata getFileMetadata(PathItem file) {
    String path = AzurePinotFSUtil.convertAzureStylePathToUriStylePath(file.getName());
    return new FileMetadata.Builder().setFilePath(path)
        .setLastModifiedTime(file.getLastModified().toInstant().toEpochMilli()).setLength(file.getContentLength())
        .setIsDirectory(file.isDirectory()).build();
  }

  /**
   * Copy a file from ADL to local location.
   *
   * @param srcUri location of the file.
   * @param dstFile location to move the file to.
   * @return nothing.
   */
  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    LOGGER.debug("copyToLocalFile is called with srcUri='{}', dstFile='{}'", srcUri, dstFile);
    if (dstFile.exists()) {
      if (dstFile.isDirectory()) {
        FileUtils.deleteDirectory(dstFile);
      } else {
        FileUtils.deleteQuietly(dstFile);
      }
    }
    int bytesRead;
    byte[] buffer = new byte[BUFFER_SIZE];
    try (InputStream inputStream = open(srcUri)) {
      try (OutputStream outputStream = new FileOutputStream(dstFile)) {
        while ((bytesRead = inputStream.read(buffer)) != -1) {
          outputStream.write(buffer, 0, bytesRead);
        }
      }
    }
    // If MD5 hash is available as part of path properties, verify it with the local file
    if (_enableChecksum) {
      DataLakeFileClient fileClient =
          _fileSystemClient.getFileClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(srcUri));
      byte[] md5ContentFromMetadata = fileClient.getProperties().getContentMd5();
      if (md5ContentFromMetadata != null && md5ContentFromMetadata.length > 0) {
        byte[] md5FromLocalFile = computeContentMd5(dstFile);
        if (!Arrays.equals(md5FromLocalFile, md5ContentFromMetadata)) {
          // Clean up the corrupted file
          FileUtils.deleteQuietly(dstFile);
          throw new IOException("Computed MD5 and MD5 from metadata do not match");
        }
      }
    }
  }

  /**
   * Copy a local file to the destination location in ADL.
   *
   * @param srcFile location of the file locally
   * @param dstUri location to move the file to.
   * @return nothing.
   */
  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    LOGGER.debug("copyFromLocalFile is called with srcFile='{}', dstUri='{}'", srcFile, dstUri);
    byte[] contentMd5 = computeContentMd5(srcFile);
    try (InputStream fileInputStream = new FileInputStream(srcFile)) {
      copyInputStreamToDst(fileInputStream, dstUri, contentMd5);
    }
  }

  /**
   * Check if a given location is a directory.
   *
   * @param uri location make the check.
   * @return true if it's a directory else false.
   */
  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    try {
      PathProperties pathProperties = getPathProperties(uri);
      Map<String, String> metadata = pathProperties.getMetadata();
      // TODO: need to find the other ways to check the directory if it becomes available. listFiles API returns
      // PathInfo, which includes "isDirectory" field; however, there's no API available for fetching PathInfo directly
      // from target uri.
      return Boolean.valueOf(metadata.get(IS_DIRECTORY_KEY));
    } catch (DataLakeStorageException e) {
      throw new IOException("Failed while checking isDirectory for : " + uri, e);
    }
  }

  /**
   * Get the last modified time of the given file location.
   *
   * @param uri location of the file to get the last modified time.
   * @return the last modified time of the target file.
   */
  @Override
  public long lastModified(URI uri)
      throws IOException {
    try {
      PathProperties pathProperties = getPathProperties(uri);
      OffsetDateTime offsetDateTime = pathProperties.getLastModified();
      return offsetDateTime.toInstant().toEpochMilli();
    } catch (DataLakeStorageException e) {
      throw new IOException("Failed while checking lastModified time for : " + uri, e);
    }
  }

  /**
   * Touch (access) a given file.
   *
   * @param uri location of the file to touch the file
   * @return true if touch succeeds else false.
   */
  @Override
  public boolean touch(URI uri)
      throws IOException {
    // The following data lake gen2 API provides a way to update file properties including last modified time.
    // https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    // However, action = "setProperties" is available in REST API but not available in Java SDK yet.
    //
    // For now, directly use Blob service's API to get the same effect.
    // https://docs.microsoft.com/en-us/rest/api/storageservices/set-file-properties
    try {
      DataLakeFileClient fileClient =
          _fileSystemClient.getFileClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri));
      PathProperties pathProperties = fileClient.getProperties();
      fileClient.setHttpHeaders(getPathHttpHeaders(pathProperties));
      return true;
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  /**
   * Open the file at a given location.
   *
   * @param uri location of the file to open.
   * @return the input stream with the contents of the file.
   */
  @Override
  public InputStream open(URI uri)
      throws IOException {
    return _fileSystemClient.getFileClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri)).openInputStream()
        .getInputStream();
  }

  private boolean copySrcToDst(URI srcUri, URI dstUri)
      throws IOException {
    PathProperties pathProperties =
        _fileSystemClient.getFileClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(srcUri)).getProperties();
    try (InputStream inputStream = open(srcUri)) {
      return copyInputStreamToDst(inputStream, dstUri, pathProperties.getContentMd5());
    }
  }

  /**
   * Helper function to copy input stream to destination URI.
   *
   * NOTE: the caller has to close the input stream.
   *
   * @param inputStream input stream that will be written to dstUri
   * @param dstUri destination URI
   * @return true if the copy succeeds
   */
  private boolean copyInputStreamToDst(InputStream inputStream, URI dstUri, byte[] contentMd5)
      throws IOException {
    int bytesRead;
    long totalBytesRead = 0;
    byte[] buffer = new byte[BUFFER_SIZE];
    // TODO: the newer client now has the API 'uploadFromFile' that directly takes the file as an input. We can replace
    // this upload logic with the 'uploadFromFile'/
    DataLakeFileClient fileClient;
    try {
      fileClient = _fileSystemClient.createFile(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(dstUri));
    } catch (DataLakeStorageException e) {
      // If the path already exists, doing nothing and return true
      if (e.getStatusCode() == ALREADY_EXISTS_STATUS_CODE && e.getErrorCode().equals(PATH_ALREADY_EXISTS_ERROR_CODE)) {
        LOGGER.info("The destination path already exists and we are overwriting the file (dstUri={})", dstUri);
        fileClient = _fileSystemClient.createFile(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(dstUri), true);
      } else {
        LOGGER.error("Exception thrown while calling copy stream to destination (dstUri={}, errorStatus ={})", dstUri,
            e.getStatusCode(), e);
        throw new IOException(e);
      }
    }

    // Update MD5 metadata
    if (contentMd5 != null) {
      PathHttpHeaders pathHttpHeaders = getPathHttpHeaders(fileClient.getProperties());
      pathHttpHeaders.setContentMd5(contentMd5);
      fileClient.setHttpHeaders(pathHttpHeaders);
    }

    try {
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        byte[] md5BlockHash = null;
        if (_enableChecksum) {
          // Compute md5 for the current block
          MessageDigest md5Block = MessageDigest.getInstance("MD5");
          md5Block.update(buffer, 0, bytesRead);
          md5BlockHash = md5Block.digest();
        }
        // Upload 4MB at a time since Azure's limit for each append call is 4MB.
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer, 0, bytesRead);
        fileClient.appendWithResponse(byteArrayInputStream, totalBytesRead, bytesRead, md5BlockHash, null, null,
            Context.NONE);
        byteArrayInputStream.close();
        totalBytesRead += bytesRead;
      }
      // Call flush on ADLS Gen 2
      fileClient.flush(totalBytesRead, true);

      return true;
    } catch (DataLakeStorageException | NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  /**
   * Compute md5 hash from the file
   * @param file input file
   * @return byte array of md5 hash
   * @throws Exception
   */
  private byte[] computeContentMd5(File file)
      throws Exception {
    MessageDigest messageDigest = MessageDigest.getInstance("MD5");
    int bytesRead;
    byte[] buffer = new byte[BUFFER_SIZE];
    try (InputStream inputStream = new FileInputStream(file)) {
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        messageDigest.update(buffer, 0, bytesRead);
      }
    }
    return messageDigest.digest();
  }

  private PathProperties getPathProperties(URI uri)
      throws IOException {
    return _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri))
        .getProperties();
  }

  private PathHttpHeaders getPathHttpHeaders(PathProperties pathProperties) {
    return new PathHttpHeaders().setCacheControl(pathProperties.getCacheControl())
        .setContentDisposition(pathProperties.getContentDisposition())
        .setContentEncoding(pathProperties.getContentEncoding()).setContentMd5(pathProperties.getContentMd5())
        .setContentLanguage(pathProperties.getContentLanguage()).setContentType(pathProperties.getContentType());
  }
}
