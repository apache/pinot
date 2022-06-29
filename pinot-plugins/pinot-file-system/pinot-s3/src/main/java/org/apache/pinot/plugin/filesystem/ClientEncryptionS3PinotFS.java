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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientV2Builder;
import com.amazonaws.services.s3.AmazonS3EncryptionV2;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CryptoConfigurationV2;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.KMSEncryptionMaterials;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MetadataDirective;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import com.amazonaws.util.IOUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;


/**
 * Implementation of PinotFS for AWS S3 file system
 */
public class ClientEncryptionS3PinotFS extends BasePinotFS {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String REGION = "region";
  public static final String ENDPOINT = "endpoint";
  public static final String DISABLE_ACL_CONFIG_KEY = "disableAcl";
  public static final String SERVER_SIDE_ENCRYPTION_CONFIG_KEY = "serverSideEncryption";
  public static final String SSE_KMS_KEY_ID_CONFIG_KEY = "ssekmsKeyId";
  public static final String SSE_KMS_ENCRYPTION_CONTEXT_CONFIG_KEY = "ssekmsEncryptionContext";

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientEncryptionS3PinotFS.class);
  private static final String DELIMITER = "/";
  public static final String S3_SCHEME = "s3://";
  private static final boolean DEFAULT_DISABLE_ACL = true;
  private static final String KMS_CMK_ID = "kmsCmkId";
  private static final String AES_HEX_SECRET = "aesHexSecret";
  // S3 encryption client
  private AmazonS3EncryptionV2 _s3Client;
  private boolean _disableAcl = DEFAULT_DISABLE_ACL;
  private ServerSideEncryption _serverSideEncryption = null;
  private String _ssekmsKeyId;
  private String _ssekmsEncryptionContext;

  @Override
  public void init(PinotConfiguration config) {
    Preconditions.checkArgument(!isNullOrEmpty(config.getProperty(REGION)), "Region can't be null or empty");
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
    AWSStaticCredentialsProvider awsCredentialsProvider;
    EncryptionMaterialsProvider encryptionMaterialsProvider = null;
    if (!isNullOrEmpty(config.getProperty(ACCESS_KEY)) && !isNullOrEmpty(config.getProperty(SECRET_KEY))) {
      String accessKey = config.getProperty(ACCESS_KEY);
      String secretKey = config.getProperty(SECRET_KEY);
      awsCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
    } else {
      awsCredentialsProvider =
          new AWSStaticCredentialsProvider(DefaultAWSCredentialsProviderChain.getInstance().getCredentials());
    }
    if (config.containsKey(KMS_CMK_ID)) {
      encryptionMaterialsProvider =
          new KMSEncryptionMaterialsProvider(new KMSEncryptionMaterials(config.getProperty(KMS_CMK_ID)));
    }

    if (config.containsKey(AES_HEX_SECRET)) {
      encryptionMaterialsProvider = new StaticEncryptionMaterialsProvider(new EncryptionMaterials(
          new SecretKeySpec(BytesUtils.toBytes(config.getProperty(AES_HEX_SECRET)), "AES")
      ));
    }
    AmazonS3EncryptionClientV2Builder s3ClientBuilder = AmazonS3EncryptionClientV2Builder.standard()
        .withRegion(region)
        .withClientConfiguration(new ClientConfiguration())
        .withCredentials(awsCredentialsProvider)
        .withCryptoConfiguration(new CryptoConfigurationV2()
            .withCryptoMode(CryptoMode.StrictAuthenticatedEncryption));

    AWSKMS kmsClient = AWSKMSClientBuilder.standard().withRegion(region).build();
    if (kmsClient != null) {
      s3ClientBuilder.withKmsClient(kmsClient);
    }

    if (encryptionMaterialsProvider != null) {
      s3ClientBuilder.setEncryptionMaterialsProvider(encryptionMaterialsProvider);
    }
    if (!isNullOrEmpty(config.getProperty(ENDPOINT))) {
      s3ClientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(config.getProperty(ENDPOINT), region));
    }
    _s3Client = s3ClientBuilder.build();
  }

  public void init(AmazonS3EncryptionV2 s3Client) {
    _s3Client = s3Client;
  }

  boolean isNullOrEmpty(String target) {
    return target == null || target.isEmpty();
  }

  private ObjectMetadata getObjectMetadata(URI uri)
      throws IOException {
    URI base = getBase(uri);
    String path = sanitizePath(base.relativize(uri).getPath());
    return _s3Client.getObjectMetadata(uri.getHost(), path);
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
      return getObjectMetadata(uri) != null;
    } catch (AmazonS3Exception s3Exception) {
      if (s3Exception.getStatusCode() == 404) {
        return false;
      }
      throw new IOException(s3Exception);
    } catch (Exception e) {
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
    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
    listObjectsV2Request.withBucketName(uri.getHost());
    if (!prefix.equals(DELIMITER)) {
      listObjectsV2Request = listObjectsV2Request.withPrefix(prefix);
    }
    ListObjectsV2Result listObjectsV2Result = _s3Client.listObjectsV2(listObjectsV2Request);
    for (S3ObjectSummary s3Object : listObjectsV2Result.getObjectSummaries()) {
      if (s3Object.getKey().equals(prefix)) {
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
   * @return {@code true} if the copy operation succeeds,
   *         {@code false} otherwise
   */
  private boolean copyFile(URI srcUri, URI dstUri) {
    CopyObjectRequest copyReq =
        generateCopyObjectRequest(srcUri.getHost(), sanitizePath(srcUri.getPath()), dstUri.getHost(),
            sanitizePath(dstUri.getPath()), null);
    return _s3Client.copyObject(copyReq) != null;
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

      PutObjectRequest putObjectRequest = generatePutObjectRequest(uri.getHost(), path);
      return _s3Client.putObject(putObjectRequest) != null;
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
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(segmentUri.getHost());
        if (!prefix.equals(DELIMITER)) {
          listObjectsV2Request.withPrefix(prefix);
        }
        ListObjectsV2Result listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
        boolean deleteSucceeded = true;
        for (S3ObjectSummary s3Object : listObjectsV2Response.getObjectSummaries()) {
          DeleteObjectRequest deleteObjectRequest =
              new DeleteObjectRequest(s3Object.getBucketName(), s3Object.getKey());
          try {
            _s3Client.deleteObject(deleteObjectRequest);
          } catch (Exception e) {
            deleteSucceeded = false;
          }
        }
        return deleteSucceeded;
      } else {
        String prefix = sanitizePath(segmentUri.getPath());
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(segmentUri.getHost(), prefix);
        _s3Client.deleteObject(deleteObjectRequest);
        return true;
      }
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    if (copyDir(srcUri, dstUri)) {
      return delete(srcUri, true);
    }
    return false;
  }

  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
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
    if (isDirectory(fileUri)) {
      return true;
    }
    if (isPathTerminatedByDelimiter(fileUri)) {
      return false;
    }
    return existsFile(fileUri);
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    Preconditions.checkState(!isPathTerminatedByDelimiter(fileUri), "URI is a directory");
    ObjectMetadata s3ObjectMetadata = getObjectMetadata(fileUri);
    Preconditions.checkState((s3ObjectMetadata != null), "File '%s' does not exist", fileUri);
    return s3ObjectMetadata.getContentLength();
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
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(fileUri.getHost());
        if (!prefix.equals(DELIMITER)) {
          listObjectsV2Request.withPrefix(prefix);
        }
        if (!recursive) {
          listObjectsV2Request.withDelimiter(DELIMITER);
        }
        if (continuationToken != null) {
          listObjectsV2Request.withContinuationToken(continuationToken);
        }
        LOGGER.debug("Trying to send ListObjectsV2Request {}", listObjectsV2Request);
        ListObjectsV2Result listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
        LOGGER.debug("Getting ListObjectsV2Response: {}", listObjectsV2Response);
        List<S3ObjectSummary> filesReturned = listObjectsV2Response.getObjectSummaries();
        filesReturned.stream().forEach(object -> {
          //Only add files and not directories
          if (!object.getKey().equals(fileUri.getPath()) && !object.getKey().endsWith(DELIMITER)) {
            String fileKey = object.getKey();
            if (fileKey.startsWith(DELIMITER)) {
              fileKey = fileKey.substring(1);
            }
            builder.add(S3_SCHEME + fileUri.getHost() + DELIMITER + fileKey);
          }
        });
        isDone = !listObjectsV2Response.isTruncated();
        continuationToken = listObjectsV2Response.getNextContinuationToken();
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

    GetObjectRequest getObjectRequest = new GetObjectRequest(srcUri.getHost(), prefix);
    S3Object s3Object = _s3Client.getObject(getObjectRequest);
    IOUtils.copy(s3Object.getObjectContent(), new FileOutputStream(dstFile));
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    LOGGER.info("Copy {} from local to {}", srcFile.getAbsolutePath(), dstUri);
    URI base = getBase(dstUri);
    String prefix = sanitizePath(base.relativize(dstUri).getPath());
    PutObjectRequest putObjectRequest = generatePutObjectRequest(dstUri.getHost(), prefix);
    putObjectRequest.withFile(srcFile);
    _s3Client.putObject(putObjectRequest);
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
          new ListObjectsV2Request().withBucketName(uri.getHost()).withPrefix(prefix).withMaxKeys(2);
      ListObjectsV2Result listObjectsV2Result = _s3Client.listObjectsV2(listObjectsV2Request);
      return !listObjectsV2Result.getObjectSummaries().isEmpty();
    } catch (Exception e) {
      LOGGER.error("Could not get directory entry for {}", uri);
      return false;
    }
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return getObjectMetadata(uri).getLastModified().getTime();
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    String path = sanitizePath(uri.getPath());
    try {
      ObjectMetadata objectMetadata = getObjectMetadata(uri);
      CopyObjectRequest request = generateCopyObjectRequest(uri.getHost(), path, uri.getHost(), path,
          ImmutableMap.of("lastModified", String.valueOf(System.currentTimeMillis())));
      _s3Client.copyObject(request);
      long newUpdateTime = getObjectMetadata(uri).getLastModified().getTime();
      return newUpdateTime > objectMetadata.getLastModified().getTime();
    } catch (Exception e) {
      _s3Client.putObject(generatePutObjectRequest(uri.getHost(), path));
      return true;
    }
  }

  private PutObjectRequest generatePutObjectRequest(String bucketName, String path) {
    // Create metadata with content 0
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0L);
    PutObjectRequest putObjectRequest =
        new PutObjectRequest(bucketName, path, new ByteArrayInputStream(new byte[0]), metadata);
    if (!_disableAcl) {
      putObjectRequest.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl);
    }

    if (_serverSideEncryption != null) {
      switch (_serverSideEncryption) {
        case AWS_KMS:
          SSEAwsKeyManagementParams sseAwsKeyManagementParams =
              new SSEAwsKeyManagementParams().withAwsKmsKeyId(_ssekmsKeyId);
          if (_ssekmsEncryptionContext != null) {
            sseAwsKeyManagementParams.withAwsKmsEncryptionContext(_ssekmsEncryptionContext);
          }
          putObjectRequest.withSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
          break;
        case AES256:
          // Todo: Support AES256.
        default:
          throw new UnsupportedOperationException("Unsupported server side encryption: " + _serverSideEncryption);
      }
    }
    return putObjectRequest;
  }

  private CopyObjectRequest generateCopyObjectRequest(String srcBucket, String srcKey, String destBucket,
      String destKey, Map<String, String> metadata) {
    CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucket, srcKey, destBucket, destKey);
    if (metadata != null) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setUserMetadata(metadata);
      copyObjectRequest.withNewObjectMetadata(objectMetadata)
          .withMetadataDirective(MetadataDirective.REPLACE);
    }
    if (!_disableAcl) {
      copyObjectRequest.withCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
    }
    if (_serverSideEncryption != null) {
      SSEAwsKeyManagementParams sseAwsKeyManagementParams =
          new SSEAwsKeyManagementParams().withAwsKmsKeyId(_ssekmsKeyId);
      if (_ssekmsEncryptionContext != null) {
        sseAwsKeyManagementParams.withAwsKmsEncryptionContext(_ssekmsEncryptionContext);
      }
      copyObjectRequest.withSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
    }
    return copyObjectRequest;
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    String path = sanitizePath(uri.getPath());
    GetObjectRequest getObjectRequest = new GetObjectRequest(uri.getHost(), path);
    return _s3Client.getObject(getObjectRequest).getObjectContent();
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
