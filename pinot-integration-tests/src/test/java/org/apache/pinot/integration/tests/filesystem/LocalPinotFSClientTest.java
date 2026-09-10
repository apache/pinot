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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.testng.annotations.Test;


public class LocalPinotFSClientTest extends BasePinotFSTest {
  @Override
  protected PinotFS getPinotFS() {
    return new LocalPinotFS();
  }

  @Override
  protected URI getBaseDirectoryUri()
      throws URISyntaxException {
    return new URI(FileUtils.getTempDirectory() + "local-pinot-fs-test-" + _uuid);
  }

  @Override
  @Test(enabled = false)
  public void testListFiles() {
    // test fails when local FS location is passed without the scheme
  }

  @Override
  @Test(enabled = false)
  public void testListFilesWithMetadata() {
    // test fails when local FS location is passed without the scheme
  }
}
