package org.apache.pinot.common.utils.filesystem;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;


public class LiLocalPinotFS extends LocalPinotFS {

  public final static String HTTP_SCHEME = "http";
  public final static String FILE_SCHEME = "file";
  private static final String DELIMITER = "/";
  private final static String MINIO_ENDPOINT = "http://127.0.0.1:9000";
  private final static String ACCESS_KEY = "minioadmin";
  private final static String SECRECT_KEY = "minioadmin";
  private final static String BUCKET_NAME = "pinot-bucket";
  private final static Pattern SEGMENT_PATTERN_IN_HTTP_URL = Pattern.compile("^http?://[^/]+/segments/(.+)");
  // Pattern.compile("^http://[^/]+/segments/([^/]+/[^/]+)$");

  private S3Client _s3Client;

  @Override
  public void init(PinotConfiguration configuration) {
    AwsBasicCredentials creds = AwsBasicCredentials.create(ACCESS_KEY, SECRECT_KEY);
    _s3Client = S3Client.builder().endpointOverride(URI.create(MINIO_ENDPOINT))
        .region(Region.US_EAST_1) // any region is okay for local
        .credentialsProvider(StaticCredentialsProvider.create(creds)).build();
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    String protocol = uri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        return super.mkdir(uri);
      case HTTP_SCHEME:
        String segmentKey = extractSegmentKey(uri);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(segmentKey)
            .build();
        PutObjectResponse putObjectResponse = _s3Client.putObject(putObjectRequest, RequestBody.fromBytes(new byte[0]));
        return putObjectResponse.sdkHttpResponse().isSuccessful();
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    String protocol = segmentUri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        return super.delete(segmentUri, forceDelete);
      case HTTP_SCHEME:
        return true;
//        String segmentKey = extractSegmentKey(segmentUri);
//        try {
//          if (isDirectory(segmentUri)) {
//            if (!forceDelete) {
////              Preconditions.checkState(isEmptyDirectory(segmentUri),
////                  "ForceDelete flag is not set and directory '%s' is not empty", segmentUri);
//            }
//            String prefix = normalizeToDirectoryPrefix(segmentUri);
//            ListObjectsV2Response listObjectsV2Response;
//            ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
//                ListObjectsV2Request.builder().bucket(segmentUri.getHost());
//
//            if (prefix.equals(DELIMITER)) {
//              ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
//              listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
//            } else {
//              ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.prefix(prefix).build();
//              listObjectsV2Response = _s3Client.listObjectsV2(listObjectsV2Request);
//            }
//            boolean deleteSucceeded = true;
//            for (S3Object s3Object : listObjectsV2Response.contents()) {
//              DeleteObjectRequest deleteObjectRequest =
//                  DeleteObjectRequest.builder().bucket(segmentUri.getHost()).key(s3Object.key()).build();
//
//              DeleteObjectResponse deleteObjectResponse = _s3Client.deleteObject(deleteObjectRequest);
//
//              deleteSucceeded &= deleteObjectResponse.sdkHttpResponse().isSuccessful();
//            }
//            return deleteSucceeded;
//          } else {
//            String prefix = sanitizePath(segmentUri.getPath());
//            DeleteObjectRequest deleteObjectRequest =
//                DeleteObjectRequest.builder().bucket(segmentUri.getHost()).key(prefix).build();
//
//            DeleteObjectResponse deleteObjectResponse = _s3Client.deleteObject(deleteObjectRequest);
//
//            return deleteObjectResponse.sdkHttpResponse().isSuccessful();
//          }
//        } catch (NoSuchKeyException e) {
//          return false;
//        } catch (S3Exception e) {
//          throw e;
//        } catch (Exception e) {
//          throw new IOException(e);
//        }
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  @Override
  public boolean exists(URI fileUri) {
    String protocol = fileUri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        return super.exists(fileUri);
      case HTTP_SCHEME:
        String segmentKey = extractSegmentKey(fileUri);
        try {
          return _s3Client.headObject(b -> b.bucket(BUCKET_NAME).key(segmentKey)).sdkHttpResponse().isSuccessful();
        } catch (NoSuchKeyException e) {
          return false;
        }
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    String protocol = dstUri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        super.copyFromLocalFile(srcFile, dstUri);
        break;
      case HTTP_SCHEME:
        String segmentKey = extractSegmentKey(dstUri);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(BUCKET_NAME).key(segmentKey).build();
        _s3Client.putObject(putObjectRequest, RequestBody.fromFile(srcFile));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  @Override
  public long length(URI fileUri) {
    String protocol = fileUri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        return super.length(fileUri);
      case HTTP_SCHEME:
        String segmentKey = extractSegmentKey(fileUri);
        HeadObjectResponse headObjectResponse = _s3Client.headObject(b -> b.bucket(BUCKET_NAME).key(segmentKey));
        return headObjectResponse.contentLength();
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    String protocol = fileUri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        return super.listFiles(fileUri, recursive);
      case HTTP_SCHEME:
        String segmentKey = extractSegmentKey(fileUri);
        List<S3Object> s3ObjectList = _s3Client.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix(segmentKey)).contents();
        List<String> files = new ArrayList<>();
        for (S3Object s3Object : s3ObjectList) {
          files.add(s3Object.key());
        }
        return files.toArray(new String[0]);
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    String protocol = srcUri.getScheme();
    switch (protocol) {
      case FILE_SCHEME:
        return super.doMove(srcUri, dstUri);
      case HTTP_SCHEME:
//        LOGGER.info("Copying uri {} to uri {}", srcUri, dstUri);
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
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  private String extractSegmentKey(URI uri) {
    String protocol = uri.getScheme();
    switch (protocol) {
      case "http":
        return extractSegmentKeyFromHttpUrl(uri.toString());
      case "file":
        return extractSegmentKeyFromFileUrl(uri.getPath());
      default:
        throw new UnsupportedOperationException("Unsupported protocol: " + protocol);
    }
  }

  private String extractSegmentKeyFromHttpUrl(String uri) {
    // url example: http://localhost:20000/segments/mytable/mytable_16071_16101_3
    Matcher matcher = SEGMENT_PATTERN_IN_HTTP_URL.matcher(uri);
    if (matcher.find()) {
      String matchedString = matcher.group(1);
      if (matchedString.contains("/")) {
        return matchedString + ".gz";
      } else {
        return matchedString;
      }
//      return matcher.group(1) + ".gz";
    }
    throw new RuntimeException("Failed to extract segment key from download uri: " + uri);
  }

  private String extractSegmentKeyFromFileUrl(String path) {
    // not needed, only for integration test
    String segmentName = path.substring(path.lastIndexOf("/") + 1, path.indexOf(".tar.gz"));
    String tableName = segmentName.substring(0, segmentName.indexOf("_"));
    return tableName + "/" + segmentName;
  }

  private boolean isPathTerminatedByDelimiter(URI uri) {
    return uri.getPath().endsWith(DELIMITER);
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

  private boolean copyFile(URI srcUri, URI dstUri)
      throws IOException {
    try {
      String srcKey = extractSegmentKey(srcUri);
      String dstKey = extractSegmentKey(dstUri);
      CopyObjectResponse copyObjectResponse = _s3Client.copyObject(
          builder -> builder.sourceBucket(BUCKET_NAME).sourceKey(srcKey).destinationBucket(BUCKET_NAME)
              .destinationKey(dstKey));
      return copyObjectResponse.sdkHttpResponse().isSuccessful();
//      String encodedUrl = URLEncoder.encode(srcUri.getHost() + srcUri.getPath(), StandardCharsets.UTF_8);
//
//      String dstPath = sanitizePath(dstUri.getPath());
//      CopyObjectRequest copyReq = generateCopyObjectRequest(encodedUrl, dstUri, dstPath, null);
//      CopyObjectResponse copyObjectResponse = _s3Client.copyObject(copyReq);
//      return copyObjectResponse.sdkHttpResponse().isSuccessful();
    } catch (S3Exception e) {
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

  private CopyObjectRequest generateCopyObjectRequest(String copySource, URI dest, String path,
      Map<String, String> metadata) {
    CopyObjectRequest.Builder copyReqBuilder =
        CopyObjectRequest.builder().copySource(copySource).destinationBucket(dest.getHost()).destinationKey(path);
//    if (_storageClass != null) {
//      copyReqBuilder.storageClass(_storageClass);
//    }
    if (metadata != null) {
      copyReqBuilder.metadata(metadata).metadataDirective(MetadataDirective.REPLACE);
    }
//    if (!_disableAcl) {
      copyReqBuilder.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
//    }
//    if (_serverSideEncryption != null) {
//      copyReqBuilder.serverSideEncryption(_serverSideEncryption).ssekmsKeyId(_ssekmsKeyId);
//      if (_ssekmsEncryptionContext != null) {
//        copyReqBuilder.ssekmsEncryptionContext(_ssekmsEncryptionContext);
//      }
//    }
    return copyReqBuilder.build();
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

  private URI getBase(URI uri)
      throws IOException {
    try {
      return new URI(uri.getScheme(), uri.getHost(), null, null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }
}
