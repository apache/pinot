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
package org.apache.pinot.integration.tests.filesystem;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.pinot.plugin.filesystem.S3Config;
import org.apache.pinot.plugin.filesystem.S3PinotFS;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.testng.annotations.Test;


public class S3PinotFSClientTest extends BasePinotFSTest {

  public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
  public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
  public static final String S3_REGION = "S3_REGION";
  public static final String S3_ENABLE_FS_TESTS = "S3_ENABLE_FS_TESTS";
  public static final String S3_FS_URI = "S3_FS_URI";

  @Override
  protected PinotFS getPinotFS() {
    return new S3PinotFS();
  }

  @Override
  protected URI getBaseDirectoryUri()
      throws URISyntaxException {
    String adlsUri = getEnvVar(S3_FS_URI);
    return new URI(adlsUri + (adlsUri.endsWith("/") ? "" : "/") + "fsTest/" + _uuid);
  }

  @Override
  protected PinotConfiguration getFsConfigs() {
    PinotConfiguration configuration = super.getFsConfigs();
    configuration.setProperty(S3Config.ACCESS_KEY, getEnvVar(S3_ACCESS_KEY));
    configuration.setProperty(S3Config.SECRET_KEY, getEnvVar(S3_SECRET_KEY));
    configuration.setProperty(S3Config.REGION, getEnvVar(S3_REGION));
    return configuration;
  }

  @Override
  protected boolean disableTests() {
    // only run the tests when ADLS_ENABLE_FS_TESTS is specifically set to true
    return !Boolean.parseBoolean(getEnvVar(S3_ENABLE_FS_TESTS));
  }

  @Override
  @Test(enabled = false)
  public void testCopy()
      throws Exception {
    // test fails as S3PinotFS.sanitizePath() trims the leading delimiter due to which
    // URI object creation fails as it expects an absolute path (starting with '/')
  }

  @Override
  @Test(enabled = false)
  public void testDelete() {
    // test fails as interface expects the FS client to return false when
    // PinotFS.delete() is called on a non-empty directory and forceDelete is not set to true,
    // while the FS implementation has a check on it which throws IllegalStateException
  }

  @Override
  @Test(enabled = false)
  public void testListFiles() {
    // test fails as PinotFS.listFiles() is expected to list all the files as well as directories while
    // the S3 client only lists files and skips listing directories
  }

  @Override
  @Test(enabled = false)
  public void testListFilesWithMetadata() {
    // test fails as PinotFS.listFiles() is expected to list all the files as well as directories while
    // the S3 client only lists files and skips listing directories
  }

  @Override
  @Test(enabled = false)
  public void testOpen() {
    // test fails as interface expects the FS client to throw an IOException when
    // PinotFS.open() is called on non existent file,
    // while the S3 client throws a NoSuchKeyException which is a RuntimeException.
  }

  @Override
  @Test(enabled = false)
  public void testMove() {
    // test fails as S3PinotFS.sanitizePath() trims the leading delimiter due to which
    // URI object creation fails as it expects an absolute path (starting with '/')
  }
}
