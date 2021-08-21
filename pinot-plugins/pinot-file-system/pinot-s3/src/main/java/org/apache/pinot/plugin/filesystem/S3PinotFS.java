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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;


/**
 * Implementation of PinotFS for AWS S3 file system
 */
public class S3PinotFS extends PinotFS {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String REGION = "region";
  public static final String ENDPOINT = "endpoint";
  public static final String DISABLE_ACL_CONFIG_KEY = "disableAcl";
  public static final String SERVER_SIDE_ENCRYPTION_CONFIG_KEY = "serverSideEncryption";
  public static final String SSE_KMS_KEY_ID_CONFIG_KEY = "ssekmsKeyId";
  public static final String SSE_KMS_ENCRYPTION_CONTEXT_CONFIG_KEY = "ssekmsEncryptionContext";

  private static final Logger LOGGER = LoggerFactory.getLogger(S3PinotFS.class);
  private static final String DELIMITER = "/";
  public static final String S3_SCHEME = "s3://";
  private static final boolean DEFAULT_DISABLE_ACL = true;
  private S3Client _s3Client;
  private boolean _disableAcl = DEFAULT_DISABLE_ACL;
  private ServerSideEncryption _serverSideEncryption = null;
  private String _ssekmsKeyId;
  private String _ssekmsEncryptionContext;

  @Override
  public void init(PinotConfiguration config) {
    Preconditions.checkArgument(!isNullOrEmpty(config.getProperty(REGION)));
    String region = config.getProperty(REGION);
    _disableAcl = config.getProperty(DISABLE_ACL_CONFIG_KEY, DEFAULT_DISABLE_ACL);
    String serverSideEncryption = config.getProperty(SERVER_SIDE_ENCRYPTION_CONFIG_KEY);
    if (serverSideEncryption != null) {
      try {
        _serverSideEncryption = ServerSideEncryption.valueOf(serverSideEncryption);
      } catch (Exception e) {
        throw new UnsupportedOperationException(String
            .format("Unknown value '%s' for S3PinotFS config: 'serverSideEncryption'. Supported values are: %s",
                serverSideEncryption, Arrays.toString(ServerSideEncryption.knownValues().toArray())));
      }
      switch (_serverSideEncryption) {
        case AWS_KMS:
          _ssekmsKeyId = config.getProperty(SSE_KMS_KEY_ID_CONFIG_KEY);
          if (_ssekmsKeyId == null) {
            throw new UnsupportedOperationException(
                "Missing required config: 'sseKmsKeyId' when AWS_KMS is used for server side encryption");
          }
          _ssekmsEncryptionContext = config.getProperty(SSE_KMS_ENCRYPTION_CONTEXT_CONFIG_KEY);
          break;
        case AES256:
          // Todo: Support AES256.
        default:
          throw new UnsupportedOperationException("Unsupported server side encryption: " + _serverSideEncryption);
      }
    }
    AwsCredentialsProvider awsCredentialsProvider;

    try {
      if (!isNullOrEmpty(config.getProperty(ACCESS_KEY)) && !isNullOrEmpty(config.getProperty(SECRET_KEY))) {
        String accessKey = config.getProperty(ACCESS_KEY);
        String secretKey = config.getProperty(SECRET_KEY);
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        awsCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
      } else {
        awsCredentialsProvider = DefaultCredentialsProvider.create();
      }

      S3ClientBuilder s3ClientBuilder =
          S3Client.builder().region(Region.of(region)).credentialsProvider(awsCredentialsProvider);
      if (!isNullOrEmpty(config.getProperty(ENDPOINT))) {
        String endpoint = config.getProperty(ENDPOINT);
        try {
          s3ClientBuilder.endpointOverride(new URI(endpoint));
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }
      _s3Client = s3ClientBuilder.build();
    } catch (S3Exception e) {
      throw new RuntimeException("Could not initialize S3PinotFS", e);
    }
  }

  public void init(S3Client s3Client) {
    _s3Client = s3Client;
  }

  boolean isNullOrEmpty(String target) {
    return target == null || "".equals(target);
  }

  private HeadObjectResponse getS3ObjectMetadata(URI uri)
      throws IOException {
    URI base = getBase(uri);
    String path = sanitizePath(base.relativize(uri).getPath());
    HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(uri.getHost()).key(path).build();

    return _s3Client.headObject(headObjectRequest);
  }

  private boolean isPathTerminatedByDelimiter(URI uri) {
    return uri.getPath().endsWith(DELIMITER);
  }

  private String normalizeToDirectoryPrefix(URI uri)
      throws IOException {
    Preconditions.checkNotNull(uri, "uri is null");
    URI strippedUri = getBase(uri).relativize(uri);
    if (isPathTerminatedByDelimiter(strippedUri)) {
      return sanitizePath(strippedUri.getPath());
    }
    return sanitizePath(strippedUri.getPath() + DELIMITER);
  }

  private URI normalizeToDirectoryUri(URI uri)
      throws IOException {
    if (isPathTerminatedByDelimiter(uri)) {
      return uri;
    }
    try {
      return new URI(uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER), null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private String sanitizePath(String path) {
    path = path.replaceAll(DELIMITER + "+", DELIMITER);
    if (path.startsWith(DELIMITER) && !path.equals(DELIMITER)) {
      path = path.substring(1);
    }
    return path;
  }

  private URI getBase(URI uri)
      throws IOException {
    try {
      return new URI(uri.getScheme(), uri.getHost(), null, null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Determines if the file exists at the given path
   * @param uri file path
   * @return {@code true} if the file exists in the path
   *         {@code false} otherwise
   */
  private boolean existsFile(URI uri)
      throws IOException {
    try {
      URI base = getBase(uri);
      String path = sanitizePath(base.relativize(uri).getPath());
      HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(uri.getHost()).key(path).build();

      _s3Client.headObject(headObjectRequest);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Determines if a path is a directory that is not empty
   * @param uri The path under the S3 bucket
   * @return {@code true} if the path is a non-empty directory,
   *         {@code false} otherwise
   */
  private boolean isEmptyDirectory(URI uri)
      throws IOException {
    if (!isDirectory(uri)) {
      return false;
    }
    String prefix = normalizeToDirectoryPrefix(uri);
    boolean isEmpty = true;
    ListObjectsV2Response listObjectsV2Response;
    ListObjectsV2Request.Builder listObjectsV2RequestBuilder = ListObjectsV2Request.builder().bucket(uri.getHost());

    if (!prefix.equals(DELIMITER)) {
      listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(prefix);
    }

    ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
    listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);

    for (S3Object s3Object : listObjectsV2Response.contents()) {
      if (s3Object.key().equals(prefix)) {
        continue;
      } else {
        isEmpty = false;
        break;
      }
    }
    return isEmpty;
  }

  /**
   * Method to copy file from source to destination.
   * @param srcUri source path
   * @param dstUri destination path
   * @return {@code true} if the copy operation succeeds, i.e., response code is 200
   *         {@code false} otherwise
   */
  private boolean copyFile(URI srcUri, URI dstUri)
      throws IOException {
    try {
      String encodedUrl = null;
      try {
        encodedUrl = URLEncoder.encode(srcUri.getHost() + srcUri.getPath(), StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }

      String dstPath = sanitizePath(dstUri.getPath());
      CopyObjectRequest copyReq = generateCopyObjectRequest(encodedUrl, dstUri, dstPath, null);
      CopyObjectResponse copyObjectResponse = _s3Client.copyObject(copyReq);
      return copyObjectResponse.sdkHttpResponse().isSuccessful();
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    LOGGER.info("mkdir {}", uri);
    try {
      Preconditions.checkNotNull(uri, "uri is null");
      String path = normalizeToDirectoryPrefix(uri);
      // Bucket root directory already exists and cannot be created
      if (path.equals(DELIMITER)) {
        return true;
      }

      PutObjectRequest putObjectRequest = generatePutObjectRequest(uri, path);
      PutObjectResponse putObjectResponse = _s3Client.putObject(putObjectRequest, RequestBody.fromBytes(new byte[0]));
      return putObjectResponse.sdkHttpResponse().isSuccessful();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    LOGGER.info("Deleting uri {} force {}", segmentUri, forceDelete);
    try {
      if (isDirectory(segmentUri)) {
        if (!forceDelete) {
          Preconditions
              .checkState(isEmptyDirectory(segmentUri), "ForceDelete flag is not set and directory '%s' is not empty",
                  segmentUri);
        }
        String prefix = normalizeToDirectoryPrefix(segmentUri);
        ListObjectsV2Response listObjectsV2Response;
        ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
            ListObjectsV2Request.builder().bucket(segmentUri.getHost());

        if (prefix.equals(DELIMITER)) {
          ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
          listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
        } else {
          ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.prefix(prefix).build();
          listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
        }
        boolean deleteSucceeded = true;
        for (S3Object s3Object : listObjectsV2Response.contents()) {
          DeleteObjectRequest deleteObjectRequest =
              DeleteObjectRequest.builder().bucket(segmentUri.getHost()).key(s3Object.key()).build();

          DeleteObjectResponse deleteObjectResponse = _s3Client.deleteObject(deleteObjectRequest);

          deleteSucceeded &= deleteObjectResponse.sdkHttpResponse().isSuccessful();
        }
        return deleteSucceeded;
      } else {
        String prefix = sanitizePath(segmentUri.getPath());
        DeleteObjectRequest deleteObjectRequest =
            DeleteObjectRequest.builder().bucket(segmentUri.getHost()).key(prefix).build();

        DeleteObjectResponse deleteObjectResponse = _s3Client.deleteObject(deleteObjectRequest);

        return deleteObjectResponse.sdkHttpResponse().isSuccessful();
      }
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    if (copy(srcUri, dstUri)) {
      return delete(srcUri, true);
    }
    return false;
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri)
      throws IOException {
    LOGGER.info("Copying uri {} to uri {}", srcUri, dstUri);
    Preconditions.checkState(exists(srcUri), "Source URI '%s' does not exist", srcUri);
    if (srcUri.equals(dstUri)) {
      return true;
    }
    if (!isDirectory(srcUri)) {
      delete(dstUri, true);
      return copyFile(srcUri, dstUri);
    }
    dstUri = normalizeToDirectoryUri(dstUri);
    Path srcPath = Paths.get(srcUri.getPath());
    try {
      boolean copySucceeded = true;
      for (String filePath : listFiles(srcUri, true)) {
        URI srcFileURI = URI.create(filePath);
        String directoryEntryPrefix = srcFileURI.getPath();
        URI src = new URI(srcUri.getScheme(), srcUri.getHost(), directoryEntryPrefix, null);
        String relativeSrcPath = srcPath.relativize(Paths.get(directoryEntryPrefix)).toString();
        String dstPath = dstUri.resolve(relativeSrcPath).getPath();
        URI dst = new URI(dstUri.getScheme(), dstUri.getHost(), dstPath, null);
        copySucceeded &= copyFile(src, dst);
      }
      return copySucceeded;
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    try {
      if (isDirectory(fileUri)) {
        return true;
      }
      if (isPathTerminatedByDelimiter(fileUri)) {
        return false;
      }
      return existsFile(fileUri);
    } catch (NoSuchKeyException e) {
      return false;
    }
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    try {
      Preconditions.checkState(!isPathTerminatedByDelimiter(fileUri), "URI is a directory");
      HeadObjectResponse s3ObjectMetadata = getS3ObjectMetadata(fileUri);
      Preconditions.checkState((s3ObjectMetadata != null), "File '%s' does not exist", fileUri);
      if (s3ObjectMetadata.contentLength() == null) {
        return 0;
      }
      return s3ObjectMetadata.contentLength();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    try {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      String continuationToken = null;
      boolean isDone = false;
      String prefix = normalizeToDirectoryPrefix(fileUri);
      while (!isDone) {
        ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
            ListObjectsV2Request.builder().bucket(fileUri.getHost());
        if (!prefix.equals(DELIMITER)) {
          listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(prefix);
        }
        if (!recursive) {
          listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.delimiter(DELIMITER);
        }
        if (continuationToken != null) {
          listObjectsV2RequestBuilder.continuationToken(continuationToken);
        }
        ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
        LOGGER.debug("Trying to send ListObjectsV2Request {}", listObjectsV2Request);
        ListObjectsV2Response listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
        LOGGER.debug("Getting ListObjectsV2Response: {}", listObjectsV2Response);
        List<S3Object> filesReturned = listObjectsV2Response.contents();
        filesReturned.stream().forEach(object -> {
          //Only add files and not directories
          if (!object.key().equals(fileUri.getPath()) && !object.key().endsWith(DELIMITER)) {
            String fileKey = object.key();
            if (fileKey.startsWith(DELIMITER)) {
              fileKey = fileKey.substring(1);
            }
            builder.add(S3_SCHEME + fileUri.getHost() + DELIMITER + fileKey);
          }
        });
        isDone = !listObjectsV2Response.isTruncated();
        continuationToken = listObjectsV2Response.nextContinuationToken();
      }
      String[] listedFiles = builder.build().toArray(new String[0]);
      LOGGER.info("Listed {} files from URI: {}, is recursive: {}", listedFiles.length, fileUri, recursive);
      return listedFiles;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    LOGGER.info("Copy {} to local {}", srcUri, dstFile.getAbsolutePath());
    URI base = getBase(srcUri);
    FileUtils.forceMkdir(dstFile.getParentFile());
    String prefix = sanitizePath(base.relativize(srcUri).getPath());
    GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(srcUri.getHost()).key(prefix).build();

    _s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(dstFile));
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    LOGGER.info("Copy {} from local to {}", srcFile.getAbsolutePath(), dstUri);
    URI base = getBase(dstUri);
    String prefix = sanitizePath(base.relativize(dstUri).getPath());
    PutObjectRequest putObjectRequest = generatePutObjectRequest(dstUri, prefix);
    _s3Client.putObject(putObjectRequest, srcFile.toPath());
  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    try {
      String prefix = normalizeToDirectoryPrefix(uri);
      if (prefix.equals(DELIMITER)) {
        return true;
      }

      ListObjectsV2Request listObjectsV2Request =
          ListObjectsV2Request.builder().bucket(uri.getHost()).prefix(prefix).maxKeys(2).build();
      ListObjectsV2Response listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
      return listObjectsV2Response.hasContents();
    } catch (NoSuchKeyException e) {
      LOGGER.error("Could not get directory entry for {}", uri);
      return false;
    }
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return getS3ObjectMetadata(uri).lastModified().toEpochMilli();
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    try {
      HeadObjectResponse s3ObjectMetadata = getS3ObjectMetadata(uri);
      String encodedUrl = null;
      try {
        encodedUrl = URLEncoder.encode(uri.getHost() + uri.getPath(), StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }

      String path = sanitizePath(uri.getPath());
      CopyObjectRequest request = generateCopyObjectRequest(encodedUrl, uri, path,
          ImmutableMap.of("lastModified", String.valueOf(System.currentTimeMillis())));
      _s3Client.copyObject(request);
      long newUpdateTime = getS3ObjectMetadata(uri).lastModified().toEpochMilli();
      return newUpdateTime > s3ObjectMetadata.lastModified().toEpochMilli();
    } catch (NoSuchKeyException e) {
      String path = sanitizePath(uri.getPath());
      PutObjectRequest putObjectRequest = generatePutObjectRequest(uri, path);
      _s3Client.putObject(putObjectRequest, RequestBody.fromBytes(new byte[0]));
      return true;
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }

  private PutObjectRequest generatePutObjectRequest(URI uri, String path) {
    PutObjectRequest.Builder putReqBuilder = PutObjectRequest.builder().bucket(uri.getHost()).key(path);

    if (!_disableAcl) {
      putReqBuilder.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
    }

    if (_serverSideEncryption != null) {
      putReqBuilder.serverSideEncryption(_serverSideEncryption).ssekmsKeyId(_ssekmsKeyId);
      if (_ssekmsEncryptionContext != null) {
        putReqBuilder.ssekmsEncryptionContext(_ssekmsEncryptionContext);
      }
    }
    return putReqBuilder.build();
  }

  private CopyObjectRequest generateCopyObjectRequest(String copySource, URI dest, String path,
      Map<String, String> metadata) {
    CopyObjectRequest.Builder copyReqBuilder =
        CopyObjectRequest.builder().copySource(copySource).destinationBucket(dest.getHost()).destinationKey(path);
    if (metadata != null) {
      copyReqBuilder.metadata(metadata).metadataDirective(MetadataDirective.REPLACE);
    }
    if (!_disableAcl) {
      copyReqBuilder.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
    }
    if (_serverSideEncryption != null) {
      copyReqBuilder.serverSideEncryption(_serverSideEncryption).ssekmsKeyId(_ssekmsKeyId);
      if (_ssekmsEncryptionContext != null) {
        copyReqBuilder.ssekmsEncryptionContext(_ssekmsEncryptionContext);
      }
    }
    return copyReqBuilder.build();
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    try {
      String path = sanitizePath(uri.getPath());
      GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(uri.getHost()).key(path).build();

      return _s3Client.getObjectAsBytes(getObjectRequest).asInputStream();
    } catch (S3Exception e) {
      throw e;
    }
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
