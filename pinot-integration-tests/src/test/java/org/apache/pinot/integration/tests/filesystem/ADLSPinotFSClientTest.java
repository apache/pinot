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
import org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.testng.annotations.Test;


public class ADLSPinotFSClientTest extends BasePinotFSTest {
  private static final String ACCOUNT_NAME = "accountName";
  private static final String ACCESS_KEY = "accessKey";
  private static final String FILE_SYSTEM_NAME = "fileSystemName";
  public static final String ADLS_ACCOUNT_NAME = "ADLS_ACCOUNT_NAME";
  public static final String ADLS_ACCESS_KEY = "ADLS_ACCESS_KEY";
  public static final String ADLS_FILE_SYSTEM_NAME = "ADLS_FILE_SYSTEM_NAME";
  public static final String ADLS_FS_URI = "ADLS_FS_URI";
  public static final String ADLS_ENABLE_FS_TESTS = "ADLS_ENABLE_FS_TESTS";

  @Override
  protected PinotFS getPinotFS() {
    return new ADLSGen2PinotFS();
  }

  @Override
  protected PinotConfiguration getFsConfigs() {
    PinotConfiguration configuration = super.getFsConfigs();
    configuration.setProperty(ACCOUNT_NAME, getEnvVar(ADLS_ACCOUNT_NAME));
    configuration.setProperty(ACCESS_KEY, getEnvVar(ADLS_ACCESS_KEY));
    configuration.setProperty(FILE_SYSTEM_NAME, getEnvVar(ADLS_FILE_SYSTEM_NAME));
    return configuration;
  }

  @Override
  protected URI getBaseDirectoryUri()
      throws URISyntaxException {
    String adlsUri = getEnvVar(ADLS_FS_URI);
    return new URI(adlsUri + (adlsUri.endsWith("/") ? "" : "/") + "fsTest/" + _uuid);
  }

  @Override
  protected boolean disableTests() {
    // only run the tests when ADLS_ENABLE_FS_TESTS is specifically set to true
    return !Boolean.parseBoolean(getEnvVar(ADLS_ENABLE_FS_TESTS));
  }

  @Override
  @Test(enabled = false)
  public void testLength() {
    // test fails as interface expects the FS client to throw exception when PinotFS.length() is called on a directory,
    // while the ADLS client returns 0.
  }

  @Override
  @Test(enabled = false)
  public void testOpen() {
    // test fails as interface expects the FS client to throw an IOException when
    // PinotFS.open() is called on non existent file,
    // while the ADLS client throws a BlobStorageException which is a RuntimeException.
  }

  @Override
  @Test(enabled = false)
  public void testTouch() {
    // test fails as interface expects the FS client to create an empty file when
    // PinotFS.touch() is called on a non existent path, while the ADLS client throws IOException.
  }
}
