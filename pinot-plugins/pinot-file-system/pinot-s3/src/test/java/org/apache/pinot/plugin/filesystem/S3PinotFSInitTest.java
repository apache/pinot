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

import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;


/**
 * Unit tests for {@link S3PinotFS} initialization paths that do not require an S3 mock container.
 */
public class S3PinotFSInitTest {

  /**
   * Regression test: {@link S3PinotFS#init(S3Client, String, PinotConfiguration)} must populate the
   * {@code _s3Config} field, not just a local. A later credential refresh runs through
   * {@link S3PinotFS#initOrRefreshS3Client()}, which reads {@code _s3Config.getRegion()}; leaving
   * the field null caused a {@link NullPointerException} on the first refresh for instances
   * initialized with a caller-provided client.
   */
  @Test
  public void testInitWithProvidedClientPopulatesConfigForRefresh() {
    S3Client providedClient = S3Client.builder()
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")))
        .build();

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(S3Config.REGION, "us-east-1");
    config.setProperty(S3Config.ACCESS_KEY, "key");
    config.setProperty(S3Config.SECRET_KEY, "secret");

    S3PinotFS s3PinotFS = new S3PinotFS();
    s3PinotFS.init(providedClient, null, config);

    // Before the fix this threw NullPointerException because _s3Config was left null. The static
    // credentials above keep the rebuild fully offline (no default-provider chain / network).
    s3PinotFS.initOrRefreshS3Client();
  }
}
