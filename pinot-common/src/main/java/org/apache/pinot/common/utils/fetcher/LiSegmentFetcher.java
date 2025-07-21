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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.env.PinotConfiguration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;


public class LiSegmentFetcher implements SegmentFetcher {

  private final static String MINIO_ENDPOINT = "http://127.0.0.1:9000";
  private final static String ACCESS_KEY = "minioadmin";
  private final static String SECRECT_KEY = "minioadmin";
  private final static String BUCKET_NAME = "pinot-bucket";
  private final static Pattern SEGMENT_PATTERN_IN_HTTP_URL = Pattern.compile("^http?://[^/]+/segments/(.+)");
  // Pattern.compile("^http://[^/]+/segments/([^/]+/[^/]+)$");

  private S3Client _s3Client;

  @Override
  public void init(PinotConfiguration config) {
    AwsBasicCredentials creds = AwsBasicCredentials.create(ACCESS_KEY, SECRECT_KEY);
    _s3Client = S3Client.builder().endpointOverride(URI.create(MINIO_ENDPOINT))
        .region(Region.US_EAST_1) // any region is okay for local
        .credentialsProvider(StaticCredentialsProvider.create(creds)).build();
  }

  @Override
  public void fetchSegmentToLocal(URI uri, File dest)
      throws Exception {
    String segmentKey = extractSegmentKey(uri);
    _s3Client.getObject(b -> b.bucket(BUCKET_NAME).key(segmentKey), dest.toPath());
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
      return matcher.group(1) + ".gz";
    }
    throw new RuntimeException("Failed to extract segment key from download uri: " + uri);
  }

  private String extractSegmentKeyFromFileUrl(String path) {
    // not needed, only for integration test
    String segmentName = path.substring(path.lastIndexOf("/") + 1, path.indexOf(".tar.gz"));
    String tableName = segmentName.substring(0, segmentName.indexOf("_"));
    return tableName + "/" + segmentName;
  }

  @Override
  public File fetchUntarSegmentToLocalStreamed(URI uri, File dest, long rateLimit, AtomicInteger attempts)
      throws Exception {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void fetchSegmentToLocal(List<URI> uri, File dest)
      throws Exception {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void fetchSegmentToLocal(String segmentName, Supplier<List<URI>> uriSupplier, File dest)
      throws Exception {
    throw new UnsupportedOperationException("Not implemented");
  }
}
