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
package org.apache.pinot.verifier;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DepVerifierTest {

  @Test
  public void testDetectsHardcodedVersionInPomTrue() throws Exception {
    InputStream inputStream = getClass().getClassLoader()
        .getResourceAsStream("test-root-pom-hardcoded.xml");

    Assert.assertNotNull(inputStream, "Test file not found");

    boolean foundHardcoded = false;
    int lineNumber = 0;
    List<Integer> lineNumberList = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        if (line.contains("<version>") && DepVerifier.isHardcoded(line)) {
          System.out.println("Hardcoded version at line " + lineNumber + ": " + line.trim());
          foundHardcoded = true;
        }
      }
    }

    Assert.assertTrue(foundHardcoded, "No hardcoded version detected in the root POM file.");
  }

  @Test
  public void testDetectsHardcodedVersionInPomFalse() throws Exception {
    InputStream inputStream = getClass().getClassLoader()
        .getResourceAsStream("test-root-pom-no-hardcode.xml");

    Assert.assertNotNull(inputStream, "Test file not found");

    boolean foundHardcoded = false;
    int lineNumber = 0;
    List<Integer> lineNumberList = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        if (line.contains("<version>") && DepVerifier.isHardcoded(line)) {
          System.out.println("Hardcoded version at line " + lineNumber + ": " + line.trim());
          foundHardcoded = true;
        }
      }
    }

    Assert.assertFalse(foundHardcoded, "Hardcoded version detected in the root POM file.");
  }
}
