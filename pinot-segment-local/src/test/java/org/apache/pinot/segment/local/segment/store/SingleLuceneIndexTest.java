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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SingleLuceneIndexTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SingleLuceneIndexTest.class.getSimpleName());

  @BeforeMethod
  public void setUp()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testIndexWithMultipleColumns()
      throws IOException {
    ArrayList<String> columns = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      columns.add("column_" + i);
    }

    TextIndexConfig config = new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null, null,
        null, null, false, false, 0, false, null);
    try (MultiColumnLuceneTextIndexCreator creator = new MultiColumnLuceneTextIndexCreator(columns, TEMP_DIR, true,
        false, null, null,
        config)) {

      ArrayList<String> values = new ArrayList<>();
      for (int row = 0; row < 1000; row++) {
        values.clear();
        for (int col = 0; col < columns.size(); col++) {
          values.add("row: " + row + "col: " + col + " value: " + (row * col));
        }
        creator.add(values);
      }

      creator.seal();
      ArrayList<String> indexFiles = getIndexFiles();
      Assert.assertEquals(indexFiles.size(), 5);
      logFiles(indexFiles);
    }
  }

  @Test
  public void testMultipleSingleColumnIndexes()
      throws IOException {
    TextIndexConfig config = new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null, null,
        null, null, false, false, 0, false, null);

    List<LuceneTextIndexCreator> creators = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      creators.add(new LuceneTextIndexCreator("column_" + i, TEMP_DIR, true, false, null, null, config));
    }

    try {
      for (int row = 0; row < 1000; row++) {
        for (int col = 0; col < creators.size(); col++) {
          creators.get(col).add("row: " + row + "col: " + col + " value: " + (row * col));
        }
      }

      for (int col = 0; col < creators.size(); col++) {
        creators.get(col).seal();
      }

      ArrayList<String> indexFiles = getIndexFiles();
      Assert.assertEquals(indexFiles.size(), 1000);
      logFiles(indexFiles);
    } finally {

      for (int col = 0; col < creators.size(); col++) {
        creators.get(col).close();
      }
    }
  }

  private void logFiles(List<String> allFiles) {
    System.out.println("Index file count: " + allFiles.size());
    System.out.println("Index files: " + allFiles);
  }

  private static ArrayList<String> getIndexFiles() {
    File[] dirs = TEMP_DIR.listFiles();
    ArrayList<String> allFiles = new ArrayList<>();
    for (int i = 0; i < dirs.length; i++) {
      File[] files = dirs[i].listFiles();
      for (int j = 0; j < files.length; j++) {
        allFiles.add(files[j].getPath());
      }
    }
    return allFiles;
  }
}
