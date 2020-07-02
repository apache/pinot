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

import java.io.IOException;
import java.net.URI;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HadoopPinotFSTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopPinotFSTest.class);

  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

  @Test
  public void testCopy()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/HadoopPinotFSTest");
    HadoopPinotFS hadoopFS = new HadoopPinotFS();
    hadoopFS.init(new BaseConfiguration());
    hadoopFS.mkdir(new Path(baseURI.getPath(), "src").toUri());
    hadoopFS.mkdir(new Path(baseURI.getPath(), "src/dir").toUri());
    hadoopFS.touch(new Path(baseURI.getPath(), "src/dir/1").toUri());
    hadoopFS.touch(new Path(baseURI.getPath(), "src/dir/2").toUri());
    String[] srcFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "src").toUri(), true);
    Assert.assertEquals(srcFiles.length, 3);
    hadoopFS.copy(new Path(baseURI.getPath(), "src").toUri(), new Path(baseURI.getPath(), "dest").toUri());
    Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest").toUri()));
    Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir").toUri()));
    Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/1").toUri()));
    Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/2").toUri()));
    String[] destFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "dest").toUri(), true);
    Assert.assertEquals(destFiles.length, 3);
    hadoopFS.delete(baseURI, true);
  }
}
