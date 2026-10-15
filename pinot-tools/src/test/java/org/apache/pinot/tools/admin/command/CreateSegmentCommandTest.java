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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class CreateSegmentCommandTest {
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = new File(FileUtils.getTempDirectory(), "CreateSegmentCommandTest_" + System.nanoTime());
    File nestedDir = new File(_tempDir, "nested");
    FileUtils.forceMkdir(nestedDir);
    FileUtils.write(new File(_tempDir, "top.csv"), "a,b\n1,2\n", "UTF-8");
    FileUtils.write(new File(nestedDir, "nested.csv"), "a,b\n3,4\n", "UTF-8");
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testSearchRecursivelyDefaultTrueFindsNestedFiles() {
    CreateSegmentCommand command = new CreateSegmentCommand().setFormat(FileFormat.CSV);
    List<String> dataFiles = command.getDataFiles(_tempDir);
    assertEquals(dataFiles.size(), 2, "Default behavior should search recursively and find both files");
    assertTrue(dataFiles.stream().anyMatch(f -> f.endsWith("top.csv")));
    assertTrue(dataFiles.stream().anyMatch(f -> f.endsWith("nested.csv")));
  }

  @Test
  public void testSearchRecursivelyFalseSkipsNestedFiles() {
    CreateSegmentCommand command =
        new CreateSegmentCommand().setFormat(FileFormat.CSV).setSearchRecursively(false);
    List<String> dataFiles = command.getDataFiles(_tempDir);
    assertEquals(dataFiles.size(), 1, "Non-recursive search should only find the top-level file");
    assertTrue(dataFiles.get(0).endsWith("top.csv"));
  }

  @Test
  public void testSearchRecursivelyTrueExplicitFindsNestedFiles() {
    CreateSegmentCommand command =
        new CreateSegmentCommand().setFormat(FileFormat.CSV).setSearchRecursively(true);
    List<String> dataFiles = command.getDataFiles(_tempDir);
    assertEquals(dataFiles.size(), 2, "Explicit true should still search recursively");
  }

  @Test
  public void testCliParsingDefaultIsRecursive() {
    CreateSegmentCommand command = new CreateSegmentCommand();
    new CommandLine(command).parseArgs("-dataDir", _tempDir.getPath(), "-format", "CSV");
    List<String> dataFiles = command.getDataFiles(_tempDir);
    assertEquals(dataFiles.size(), 2, "CLI without -searchRecursively should default to recursive search");
    assertTrue(dataFiles.stream().anyMatch(f -> f.endsWith("top.csv")));
    assertTrue(dataFiles.stream().anyMatch(f -> f.endsWith("nested.csv")));
  }

  @Test
  public void testCliParsingSearchRecursivelyFalse() {
    CreateSegmentCommand command = new CreateSegmentCommand();
    new CommandLine(command).parseArgs("-dataDir", _tempDir.getPath(), "-format", "CSV",
        "-searchRecursively", "false");
    List<String> dataFiles = command.getDataFiles(_tempDir);
    assertEquals(dataFiles.size(), 1, "CLI '-searchRecursively false' must actually disable recursion");
    assertTrue(dataFiles.get(0).endsWith("top.csv"));
  }

  @Test
  public void testCliParsingSearchRecursivelyTrueExplicit() {
    CreateSegmentCommand command = new CreateSegmentCommand();
    new CommandLine(command).parseArgs("-dataDir", _tempDir.getPath(), "-format", "CSV",
        "-searchRecursively", "true");
    List<String> dataFiles = command.getDataFiles(_tempDir);
    assertEquals(dataFiles.size(), 2, "CLI '-searchRecursively true' should keep recursive search");
  }
}
