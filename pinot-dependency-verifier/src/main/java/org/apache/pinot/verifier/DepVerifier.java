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

import java.io.File;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
// dont use *

public class DepVerifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(DepVerifier.class);

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      LOGGER.info("No changed POM files detected. Skipping dependency verification.");
      return;
    }

    for (String pomPath: args) {
      List<String> addedLines = getAddedLines(pomPath);
      for (String line: addedLines) {
        if (line.contains("<version>") && isHardcoded(line)) {
          LOGGER.error("Hardcoded version found in root POM 'dependencyManagement'. Line: {}", line.trim());
          System.exit(1);
        }
      }
    }

    System.exit(0);
  }

  private List<String> getAddedLines(String pomPath) {
    List<String> added = new ArrayList<>();

    ProcessBuilder pb = new ProcessBuilder("git", "diff", "origin/main", "--", filePath);
    Process process = pb.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    String line;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith("+") && !line.startsWith("+++")) {
        added.add(line);
      }
    }

    process.waitFor();
    return added;
  }

  private boolean isHardcoded(String line) throws Exception {
    line = line.trim();
    return line.contains("<version>") &&
        !line.contains("<version>${");
  }
}
