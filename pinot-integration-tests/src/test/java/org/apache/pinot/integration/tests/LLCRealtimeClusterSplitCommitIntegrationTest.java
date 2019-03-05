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
package org.apache.pinot.integration.tests;

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.filesystem.LocalPinotFS;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;


/**
 * Integration test that extends LLCRealtimeClusterIntegrationTest but with split commit enabled.
 */
public class LLCRealtimeClusterSplitCommitIntegrationTest extends LLCRealtimeClusterIntegrationTest {

  @Override
  public void startController() {
    ControllerConf controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.setSplitCommit(true);
    startController(controllerConfig);
  }

  @Override
  public void startServer() {
    Configuration serverConfig = getDefaultServerConfiguration();
    serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
    startServer(serverConfig);
  }
  // The goals of the following tests to show the diff between org.apache.commons.httpclient.URI and java.net.URI.
  // They can be deleted on this PR submission.
  // testURIEquivalenceForTableWithEscapeChars() test shows org.apache.commons.httpclient.URI does not work for escape
  // char like %.
  @Test
  public void testURIEquivalence() throws Exception {
    String tableName = "mytable" + String.valueOf(System.currentTimeMillis());
    String SCHEME = "file://";
    // 1. Make a temporary file
    File f = File.createTempFile("segment", "tmp");

    LocalPinotFS localPinotFS = new LocalPinotFS();
    URI segDirURI = new URI(StringUtil.join("/", System.getProperty("java.io.tmpdir"), tableName));
    URI segFileURI = new URI(StringUtil.join("/", System.getProperty("java.io.tmpdir"), tableName, f.getName()));
    Assert.assertTrue(localPinotFS.mkdir(segDirURI));
    // 2. Copy the file to Pinot FS.
    localPinotFS.copyFromLocalFile(f, segFileURI);

    // 3. Verify that the URIs exist for both apache URI and java.net URI.
    String fileStr = SCHEME + segFileURI.toString();
    org.apache.commons.httpclient.URI apacheURI =
            new org.apache.commons.httpclient.URI(fileStr, false);

    Assert.assertTrue(localPinotFS.exists(new URI(apacheURI.toString())));
    Assert.assertTrue(localPinotFS.exists(URI.create(fileStr)));

    localPinotFS.delete(segDirURI,true);
  }

  @Test
  public void testURIEquivalenceForTableWithEscapeChars() throws Exception {
    String tableName = "mytable%" + String.valueOf(System.currentTimeMillis());
    String SCHEME = "file://";
    // 1. Make a temporary file
    File f = File.createTempFile("segment", "tmp");

    LocalPinotFS localPinotFS = new LocalPinotFS();
    URI segDirURI = new URI(StringUtil.join("/", System.getProperty("java.io.tmpdir"), tableName));
    URI segFileURI = new URI(StringUtil.join("/", System.getProperty("java.io.tmpdir"), tableName, f.getName()));
    Assert.assertTrue(localPinotFS.mkdir(segDirURI));
    // 2. Copy the file to Pinot FS.
    localPinotFS.copyFromLocalFile(f, segFileURI);

    // 3. Verify that the URI exists for the java.net but not the Apache converted URI.
    String fileStr = SCHEME + segFileURI.toString();
    org.apache.commons.httpclient.URI apacheURI =
            new org.apache.commons.httpclient.URI(fileStr, false);

    Assert.assertFalse(localPinotFS.exists(new URI(apacheURI.toString())));
    Assert.assertTrue(localPinotFS.exists(URI.create(fileStr)));

    localPinotFS.delete(segDirURI,true);
  }
}
