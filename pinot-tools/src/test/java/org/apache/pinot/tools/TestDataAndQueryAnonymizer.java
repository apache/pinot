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
package org.apache.pinot.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.tools.anonymizer.PinotDataAndQueryAnonymizer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDataAndQueryAnonymizer {
  private static final String ORIGINAL_QUERY_FILE_NAME = "queries.raw";
  private static final String GENERATED_QUERY_FILE_NAME = "queries.generated";

  private String getQueryDir() {
    URL resourceUrl = getClass().getClassLoader().getResource(ORIGINAL_QUERY_FILE_NAME);
    File file = new File(resourceUrl.getFile());
    String path = file.getAbsolutePath();
    return path.substring(0, path.indexOf(ORIGINAL_QUERY_FILE_NAME) - 1);
  }

  @Test
  public void testFilterColumnExtractor()
      throws Exception {
    Set<String> filterColumns = PinotDataAndQueryAnonymizer.FilterColumnExtractor
        .extractColumnsUsedInFilter(getQueryDir(), ORIGINAL_QUERY_FILE_NAME);
    Assert.assertEquals(7, filterColumns.size());
    Assert.assertTrue(filterColumns.contains("C9"));
    Assert.assertTrue(filterColumns.contains("C10"));
    Assert.assertTrue(filterColumns.contains("C11"));
    Assert.assertTrue(filterColumns.contains("C2"));
    Assert.assertTrue(filterColumns.contains("C3"));
    Assert.assertTrue(filterColumns.contains("C12"));
    Assert.assertTrue(filterColumns.contains("C13"));
  }

  @Test
  public void testQueryGeneratorWithoutGlobalDictionary()
      throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource(ORIGINAL_QUERY_FILE_NAME);
    File queryFile = new File(resourceUrl.getFile());
    String pathToQueryFile = queryFile.getAbsolutePath();
    String pathToQueryDir = pathToQueryFile.substring(0, pathToQueryFile.indexOf(ORIGINAL_QUERY_FILE_NAME) - 1);
    String[] origQueries = new String[100];
    int queryCount = 0;
    try (InputStream inputStream = new FileInputStream(queryFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String query;
      while ((query = reader.readLine()) != null) {
        origQueries[queryCount++] = query;
      }
    }

    resourceUrl = getClass().getClassLoader().getResource(GENERATED_QUERY_FILE_NAME);
    File generatedQueryFile = new File(resourceUrl.getFile());

    String[] generatedQueries = new String[queryCount];
    int count = 0;
    try (InputStream inputStream = new FileInputStream(generatedQueryFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String query;
      while ((query = reader.readLine()) != null) {
        generatedQueries[count++] = query;
      }
    }

    Set<String> filterColumns = PinotDataAndQueryAnonymizer.FilterColumnExtractor
        .extractColumnsUsedInFilter(pathToQueryDir, ORIGINAL_QUERY_FILE_NAME);
    Set<String> timeColumns = new HashSet<>();
    // take 4 of 5 columns as time columns (data retained for these columns)
    // thus the query generator will use the predicate literal value as is
    // when rewriting predicates since there is no global dictionary for these
    // columns. C11 (the 5th predicate column) can be rewritten using global
    // dictionary
    timeColumns.add("C2");
    timeColumns.add("C3");
    timeColumns.add("C9");
    timeColumns.add("C10");
    timeColumns.add("C12");
    timeColumns.add("C13");
    PinotDataAndQueryAnonymizer.QueryGenerator queryGenerator =
        new PinotDataAndQueryAnonymizer.QueryGenerator(getQueryDir(), getQueryDir(), ORIGINAL_QUERY_FILE_NAME,
            "MyTable", filterColumns, timeColumns);

    for (int i = 0; i < queryCount; i++) {
      String generatedQuery = queryGenerator.generateQuery(origQueries[i], null);
      Assert.assertEquals(generatedQueries[i], generatedQuery);
    }
  }
}
