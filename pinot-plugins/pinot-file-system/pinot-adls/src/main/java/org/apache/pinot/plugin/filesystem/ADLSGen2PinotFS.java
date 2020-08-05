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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
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


/**
 * Azure Data Lake Storage Gen2 implementation for the PinotFS interface.
 *
 * TODO: add the unit test
 */
public class ADLSGen2PinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(ADLSGen2PinotFS.class);

  private static final String ACCOUNT_NAME = "accountName";
  private static final String ACCESS_KEY = "accessKey";
  private static final String FILE_SYSTEM_NAME = "fileSystemName";
  private static final String ENABLE_CHECKSUM = "enableChecksum";

  private static final String HTTPS_URL_PREFIX = "https://";

  private static final String AZURE_STORAGE_DNS_SUFFIX = ".dfs.core.windows.net";
  private static final String AZURE_BLOB_DNS_SUFFIX = ".blob.core.windows.net";
  private static final String PATH_ALREADY_EXISTS_ERROR_CODE = "PathAlreadyExists";
  private static final String IS_DIRECTORY_KEY = "hdi_isfolder";

  private static final int NOT_FOUND_STATUS_CODE = 404;
  private static final int ALREADY_EXISTS_STATUS_CODE = 409;

  // Azure Data Lake Gen2's block size is 4MB
  private static final int BUFFER_SIZE = 4 * 1024 * 1024;

  private DataLakeFileSystemClient _fileSystemClient;
  private BlobServiceClient _blobServiceClient;

  // If enabled, pinotFS implementation will guarantee that the bits you've read are the same as the ones you wrote.
  // However, there's some overhead in computing hash. (Adds roughly 3 seconds for 1GB file)
  private boolean _enableChecksum;

  @Override
  public void init(PinotConfiguration config) {
    _enableChecksum = config.getProperty(ENABLE_CHECKSUM, false);

    // Azure storage account name
    String accountName = config.getProperty(ACCOUNT_NAME);

    // TODO: consider to add the encryption of the following config
    String accessKey = config.getProperty(ACCESS_KEY);
    String fileSystemName = config.getProperty(FILE_SYSTEM_NAME);

    String dfsServiceEndpointUrl = HTTPS_URL_PREFIX + accountName + AZURE_STORAGE_DNS_SUFFIX;
    String blobServiceEndpointUrl = HTTPS_URL_PREFIX + accountName + AZURE_BLOB_DNS_SUFFIX;

    StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accessKey);

    DataLakeServiceClient serviceClient = new DataLakeServiceClientBuilder().credential(sharedKeyCredential)
        .endpoint(dfsServiceEndpointUrl)
        .buildClient();

    _blobServiceClient =
        new BlobServiceClientBuilder().credential(sharedKeyCredential).endpoint(blobServiceEndpointUrl).buildClient();
    _fileSystemClient = serviceClient.getFileSystemClient(fileSystemName);
    LOGGER.info("ADLSGen2PinotFS is initialized (accountName={}, fileSystemName={}, dfsServiceEndpointUrl={}, "
            + "blobServiceEndpointUrl={}, enableChecksum={})", accountName, fileSystemName, dfsServiceEndpointUrl,
        blobServiceEndpointUrl, _enableChecksum);
  }

  @Override
  public boolean mkdir(URI uri) throws IOException {
    LOGGER.debug("mkdir is called with uri='{}'", uri);
    try {
      // By default, create directory call will overwrite if the path already exists. Setting IfNoneMatch = "*" to
      // prevent overwrite. https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
      DataLakeRequestConditions requestConditions = new DataLakeRequestConditions().setIfNoneMatch("*");
      _fileSystemClient.createDirectoryWithResponse(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri), null,
          null, null, null, requestConditions, null, null);
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

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete) throws IOException {
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

  @Override
  public boolean doMove(URI srcUri, URI dstUri) throws IOException {
    LOGGER.debug("doMove is called with srcUri='{}', dstUri='{}'", srcUri, dstUri);
    try {
      // TODO: currently, azure-sdk has a bug in "rename" when the path includes some special characters that gets
      // changed during the url encoding (e.g '%' -> '%25', ' ' -> '%20')
      // https://github.com/Azure/azure-sdk-for-java/issues/8761
      DataLakeDirectoryClient directoryClient =
          _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(srcUri));
      directoryClient.rename(null, AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(dstUri));
      return true;
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
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

  @Override
  public boolean exists(URI fileUri) throws IOException {
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

  @Override
  public long length(URI fileUri) throws IOException {
    try {
      PathProperties pathProperties =
          _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(fileUri))
              .getProperties();
      return pathProperties.getFileSize();
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
    LOGGER.debug("listFiles is called with fileUri='{}', recursive='{}'", fileUri, recursive);
    try {
      // Unlike other Azure SDK APIs that takes url encoded path, ListPathsOptions takes decoded url
      // e.g) 'path/segment' instead of 'path%2Fsegment'
      String pathForListPathsOptions =
          Utility.urlDecode(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(fileUri));
      ListPathsOptions options = new ListPathsOptions().setPath(pathForListPathsOptions).setRecursive(recursive);
      PagedIterable<PathItem> iter = _fileSystemClient.listPaths(options, null);
      return iter.stream()
          .map(p -> AzurePinotFSUtil.convertAzureStylePathToUriStylePath(p.getName()))
          .toArray(String[]::new);
    } catch (DataLakeStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
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

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    LOGGER.debug("copyFromLocalFile is called with srcFile='{}', dstUri='{}'", srcFile, dstUri);
    byte[] contentMd5 = computeContentMd5(srcFile);
    try (InputStream fileInputStream = new FileInputStream(srcFile)) {
      copyInputStreamToDst(fileInputStream, dstUri, contentMd5);
    }
  }

  @Override
  public boolean isDirectory(URI uri) throws IOException {
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

  @Override
  public long lastModified(URI uri) throws IOException {
    try {
      PathProperties pathProperties = getPathProperties(uri);
      OffsetDateTime offsetDateTime = pathProperties.getLastModified();
      Timestamp timestamp = Timestamp.valueOf(offsetDateTime.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
      return timestamp.getTime();
    } catch (DataLakeStorageException e) {
      throw new IOException("Failed while checking lastModified time for : " + uri, e);
    }
  }

  @Override
  public boolean touch(URI uri) throws IOException {
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

  @Override
  public InputStream open(URI uri) throws IOException {
    // Use Blob API since read() function from Data Lake Client currently takes "OutputStream" as an input and
    // flush bytes to an output stream. This needs to be piped back into input stream to implement this function.
    // On the other hand, Blob API directly allow you to open the input stream.
    BlobClient blobClient = _blobServiceClient.getBlobContainerClient(_fileSystemClient.getFileSystemName())
        .getBlobClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri));

    return blobClient.openInputStream();
    // Another approach is to download the file to the local disk to a temp path and return the file input stream. In
    // this case, we need to override "close()" and delete temp file.
  }

  private boolean copySrcToDst(URI srcUri, URI dstUri) throws IOException {
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
    DataLakeFileClient fileClient =
        _fileSystemClient.createFile(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(dstUri));

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
      fileClient.flush(totalBytesRead);

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
  private byte[] computeContentMd5(File file) throws Exception{
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

  private PathProperties getPathProperties(URI uri) throws IOException {
    return _fileSystemClient.getDirectoryClient(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(uri))
        .getProperties();
  }

  private PathHttpHeaders getPathHttpHeaders(PathProperties pathProperties) {
    return new PathHttpHeaders().setCacheControl(pathProperties.getCacheControl())
        .setContentDisposition(pathProperties.getContentDisposition())
        .setContentEncoding(pathProperties.getContentEncoding())
        .setContentMd5(pathProperties.getContentMd5())
        .setContentLanguage(pathProperties.getContentLanguage())
        .setContentType(pathProperties.getContentType());
  }
}
