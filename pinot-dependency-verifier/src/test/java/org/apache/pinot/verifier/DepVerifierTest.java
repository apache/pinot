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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DepVerifierTest {

  @Test
  public void testIsHardcodedTrue() throws Exception {
    InputStream inputStream = getClass().getClassLoader()
        .getResourceAsStream("test-root-pom-hardcoded.xml");

    Assert.assertNotNull(inputStream, "Test file not found");

    boolean foundHardcoded = false;
    int lineNumber = 0;

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
  public void testIsHardcodedFalse() throws Exception {
    InputStream inputStream = getClass().getClassLoader()
        .getResourceAsStream("test-root-pom-no-hardcode.xml");

    Assert.assertNotNull(inputStream, "Test file not found");

    boolean foundHardcoded = false;
    int lineNumber = 0;

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

  @Test
  public void testIsInsideTagBlock() throws Exception {
    List<String> fullLines = Files.readAllLines(Paths.get("./src/test/resources/test-root-pom.xml"));

    Assert.assertTrue(DepVerifier.isInsideTagBlock(60, fullLines, "dependencyManagement"),
        "Line 60 should be inside <dependencyMangement>");
    Assert.assertFalse(DepVerifier.isInsideTagBlock(50, fullLines, "dependencyManagement"),
        "Line 50 should be outside <dependencyMangement>");
    Assert.assertFalse(DepVerifier.isInsideTagBlock(83, fullLines, "dependencyManagement"),
        "Line 83 should be outside <dependencyMangement>");

    Assert.assertTrue(DepVerifier.isInsideTagBlock(50, fullLines, "plugins"), "Line 50 should be inside <plugins>");
    Assert.assertTrue(DepVerifier.isInsideTagBlock(83, fullLines, "plugins"), "Line 83 should be inside <plugins>");
    Assert.assertFalse(DepVerifier.isInsideTagBlock(65, fullLines, "plugins"), "Line 65 should be outside <plugins>");
  }
}
