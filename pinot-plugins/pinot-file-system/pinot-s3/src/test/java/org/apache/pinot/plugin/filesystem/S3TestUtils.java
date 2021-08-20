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

import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


public class S3TestUtils {
  private S3TestUtils() {
  }

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
