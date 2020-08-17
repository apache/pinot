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
import java.util.HashMap;
import java.util.Map;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
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
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;


public class S3PinotFS extends PinotFS {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String REGION = "region";

  private static final Logger LOGGER = LoggerFactory.getLogger(S3PinotFS.class);
  private static final String DELIMITER = "/";
  public static final String S3_SCHEME = "s3://";
  private S3Client _s3Client;

  @Override
  public void init(PinotConfiguration config) {
    Preconditions.checkArgument(!isNullOrEmpty(config.getProperty(REGION)));
    String region = config.getProperty(REGION);

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

      _s3Client = S3Client.builder().region(Region.of(region)).credentialsProvider(awsCredentialsProvider).build();
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
      CopyObjectRequest copyReq =
          CopyObjectRequest.builder().copySource(encodedUrl).destinationBucket(dstUri.getHost()).destinationKey(dstPath)
              .build();

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

      PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(uri.getHost()).key(path).build();

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
        String directoryEntryPrefix =  srcFileURI.getPath();
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
      String prefix = normalizeToDirectoryPrefix(fileUri);

      ListObjectsV2Response listObjectsV2Response;
      ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
          ListObjectsV2Request.builder().bucket(fileUri.getHost());

      if (!prefix.equals(DELIMITER)) {
        listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(prefix);
      }

      if (!recursive) {
        listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.delimiter(DELIMITER);
      }

      ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
      listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);

      listObjectsV2Response.contents().stream().forEach(object -> {
        //Only add files and not directories
        if (!object.key().equals(fileUri.getPath()) && !object.key().endsWith(DELIMITER)) {
          String fileKey = object.key();
          if (fileKey.startsWith(DELIMITER)) {
            fileKey = fileKey.substring(1);
          }
          builder.add(S3_SCHEME + fileUri.getHost() + DELIMITER + fileKey);
        }
      });
      return builder.build().toArray(new String[0]);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    LOGGER.info("Copy {} to local {}", srcUri, dstFile.getAbsolutePath());
    URI base = getBase(srcUri);
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
    PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(dstUri.getHost()).key(prefix).build();

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

      ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request
              .builder().bucket(uri.getHost())
              .prefix(prefix).maxKeys(2).build();
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
      Map<String, String> mp = new HashMap<>();
      mp.put("lastModified", String.valueOf(System.currentTimeMillis()));
      CopyObjectRequest request =
          CopyObjectRequest.builder().copySource(encodedUrl).destinationBucket(uri.getHost()).destinationKey(path)
              .metadata(mp).metadataDirective(MetadataDirective.REPLACE).build();

      _s3Client.copyObject(request);
      long newUpdateTime = getS3ObjectMetadata(uri).lastModified().toEpochMilli();
      return newUpdateTime > s3ObjectMetadata.lastModified().toEpochMilli();
    } catch (NoSuchKeyException e) {
      String path = sanitizePath(uri.getPath());
      _s3Client.putObject(PutObjectRequest.builder().bucket(uri.getHost()).key(path).build(),
          RequestBody.fromBytes(new byte[0]));
      return true;
    } catch (S3Exception e) {
      throw new IOException(e);
    }
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
