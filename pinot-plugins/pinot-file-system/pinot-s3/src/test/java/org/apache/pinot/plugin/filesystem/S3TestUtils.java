package org.apache.pinot.plugin.filesystem;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


public class S3TestUtils {

  public static PutObjectRequest getPutObjectRequest(String bucket, String key) {
    return PutObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static HeadObjectRequest getHeadObjectRequest(String bucket, String key) {
    return HeadObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static ListObjectsV2Request getListObjectRequest(String bucket, String key, boolean isInBucket) {
    ListObjectsV2Request.Builder listObjectsV2RequestBuilder = ListObjectsV2Request.builder().bucket(bucket);

    if (!isInBucket) {
      listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(key);
    }

    return listObjectsV2RequestBuilder.build();
  }
}
