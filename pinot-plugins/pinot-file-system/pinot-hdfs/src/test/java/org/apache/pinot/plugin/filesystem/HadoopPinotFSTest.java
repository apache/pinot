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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HadoopPinotFSTest {
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

  @Test
  public void testCopy()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/HadoopPinotFSTest");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(new Path(baseURI.getPath(), "src").toUri());
      hadoopFS.mkdir(new Path(baseURI.getPath(), "src/dir").toUri());
      hadoopFS.touch(new Path(baseURI.getPath(), "src/dir/1").toUri());
      hadoopFS.touch(new Path(baseURI.getPath(), "src/dir/2").toUri());
      String[] srcFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "src").toUri(), true);
      Assert.assertEquals(srcFiles.length, 3);
      hadoopFS.copyDir(new Path(baseURI.getPath(), "src").toUri(), new Path(baseURI.getPath(), "dest").toUri());
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/1").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/2").toUri()));
      String[] destFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "dest").toUri(), true);
      Assert.assertEquals(destFiles.length, 3);
      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testListFilesWithMetadata()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/HadoopPinotFSTestListFiles");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());

      // Create a testDir and file underneath directory
      int count = 5;
      List<String> expectedNonRecursive = new ArrayList<>();
      List<String> expectedRecursive = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        URI testDir = new Path(baseURI.getPath(), "testDir" + i).toUri();
        hadoopFS.mkdir(testDir);
        expectedNonRecursive.add((testDir.getScheme() == null ? "file:" : "") + testDir);

        URI testFile = new Path(testDir.getPath(), "testFile" + i).toUri();
        hadoopFS.touch(testFile);
        expectedRecursive.add((testDir.getScheme() == null ? "file:" : "") + testDir);
        expectedRecursive.add((testDir.getScheme() == null ? "file:" : "") + testFile);
      }
      URI testDirEmpty = new Path(baseURI.getPath(), "testDirEmpty").toUri();
      hadoopFS.mkdir(testDirEmpty);
      expectedNonRecursive.add((testDirEmpty.getScheme() == null ? "file:" : "") + testDirEmpty);
      expectedRecursive.add((testDirEmpty.getScheme() == null ? "file:" : "") + testDirEmpty);

      URI testRootFile = new Path(baseURI.getPath(), "testRootFile").toUri();
      hadoopFS.touch(testRootFile);
      expectedNonRecursive.add((testRootFile.getScheme() == null ? "file:" : "") + testRootFile);
      expectedRecursive.add((testRootFile.getScheme() == null ? "file:" : "") + testRootFile);

      // Assert that recursive list files and nonrecursive list files are as expected
      String[] files = hadoopFS.listFiles(baseURI, false);
      Assert.assertEquals(files.length, count + 2);
      Assert.assertTrue(expectedNonRecursive.containsAll(Arrays.asList(files)), Arrays.toString(files));
      files = hadoopFS.listFiles(baseURI, true);
      Assert.assertEquals(files.length, count * 2 + 2);
      Assert.assertTrue(expectedRecursive.containsAll(Arrays.asList(files)), Arrays.toString(files));

      // Assert that recursive list files and nonrecursive list files with file info are as expected
      List<FileMetadata> fileMetadata = hadoopFS.listFilesWithMetadata(baseURI, false);
      Assert.assertEquals(fileMetadata.size(), count + 2);
      Assert.assertEquals(fileMetadata.stream().filter(FileMetadata::isDirectory).count(), count + 1);
      Assert.assertEquals(fileMetadata.stream().filter(f -> !f.isDirectory()).count(), 1);
      Assert.assertTrue(expectedNonRecursive
              .containsAll(fileMetadata.stream().map(FileMetadata::getFilePath).collect(Collectors.toSet())),
          fileMetadata.toString());
      fileMetadata = hadoopFS.listFilesWithMetadata(baseURI, true);
      Assert.assertEquals(fileMetadata.size(), count * 2 + 2);
      Assert.assertEquals(fileMetadata.stream().filter(FileMetadata::isDirectory).count(), count + 1);
      Assert.assertEquals(fileMetadata.stream().filter(f -> !f.isDirectory()).count(), count + 1);
      Assert.assertTrue(expectedRecursive
              .containsAll(fileMetadata.stream().map(FileMetadata::getFilePath).collect(Collectors.toSet())),
          fileMetadata.toString());
    }
  }
}
