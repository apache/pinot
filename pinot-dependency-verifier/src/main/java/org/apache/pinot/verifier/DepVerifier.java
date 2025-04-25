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

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

public class DepVerifier {
  private static final List<String> SKIPPED_DIRS = Arrays.asList(
      "pinot-plugins",
      "pinot-connectors",
      "pinot-integration-tests",
      "pinot-tools",
      "contrib"
  );

  private DepVerifier() { }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      return;
    }


    for (String pomPath: args) {
      Set<Integer> addedVersionLineNums = getAddedVersionLineNums(pomPath);
      List<String> fullLines = Files.readAllLines(Paths.get(pomPath));

      for (int lineNum: addedVersionLineNums) {
        String line = fullLines.get(lineNum - 1);
        if (isHardcoded(line)) {
          if (isRootPom(pomPath) && isInsideTagBlock(lineNum, fullLines, "plugins")
              || !isInsideTagBlock(lineNum, fullLines, "dependencyManagement")) {
            continue;
          }
          if (!isMaven(fullLines, lineNum)) {
            System.out.println("A dependency/non-Maven plugin is defined with a hardcoded version in the file "
                + pomPath + " line " + line.trim());
            System.exit(1);
          }
        } else {
          if (isRootPom(pomPath) || isInSkippedDirs(pomPath)) {
            break;
          }
          if (isInsideTagBlock(lineNum, fullLines, "dependencyManagement")
              || isInsideTagBlock(lineNum, fullLines, "plugins")) {
            continue;
          } else {
            System.out.println("Dependency defined in submodule POM file " + pomPath + " line " + line.trim());
            System.exit(1);
          }
        }
      }
    }

    System.exit(0);
  }

  /**
   * Checks if the given pomPath is the root pom.xml
   */
  public static boolean isRootPom(String pomPath) {
    return pomPath.equals("pom.xml");
  }

  public static boolean isInsideTagBlock(int lineNum, List<String> fullLines, String tag)
      throws IOException, InterruptedException {
    for (int i = lineNum - 1; i >= 0; i--) {
      String line = fullLines.get(i).trim();
      if (line.equals("</" + tag + ">")) {
        return false;
      }
      if (line.equals("<" + tag + ">")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Given the line number and the full file content, find the corresponding artifact ID.
   */
  public static boolean isMaven(List<String> fullLines, int lineNum) {
    for (int i = lineNum - 1; i >= 0; i--) {
      String line = fullLines.get(i).trim();
      if (line.startsWith("<artifactId>") && line.endsWith("</artifactId>")) {
        return line.contains("maven");
      }
    }
    return false;
  }

  /**
   * Returns the list of number of added lines with <version> tags to any POM files
   */
  public static Set<Integer> getAddedVersionLineNums(String pomPath) throws IOException, InterruptedException {
    Set<Integer> addedLineNums = new HashSet<>();

    ProcessBuilder pb = new ProcessBuilder("git", "diff", "-U0", "origin/main", "--", pomPath);
    Process process = pb.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    String line;
    int currentLineNum = -1;

    while ((line = reader.readLine()) != null) {
      if (line.startsWith("@@")) {
        String[] parts = line.split(" ");
        String addedRange = parts[2];
        String[] range = addedRange.substring(1).split(",");
        currentLineNum = Integer.parseInt(range[0]) - 1;
      } else if (line.startsWith("+") && !line.startsWith("+++")) {
        currentLineNum++;
        String content = line.substring(1).trim();
        if (content.contains("<version>")) {
          addedLineNums.add(currentLineNum);
        }
      }
    }

    process.waitFor();
    return addedLineNums;
  }


  /**
   * Checks if a given line contains a hardcoded version.
   */
  public static boolean isHardcoded(String line) throws Exception {
    line = line.trim();
    if (line.contains("1.4.0-SNAPSHOT") || line.contains("<!-- @dependabot ignore -->")) {
      return false;
    }

    return line.contains("<version>") && !line.matches(".*<version>\\$\\{[^}]+}</version>.*");
  }

  public static boolean isInSkippedDirs(String pomPath) {
    return SKIPPED_DIRS.stream().anyMatch(pomPath::contains);
  }
}
