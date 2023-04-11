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
package org.apache.pinot.tools.admin.command.filesystem;

import java.io.IOException;
import java.net.URI;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.StringUtil;
import picocli.CommandLine;


@CommandLine.Command(name = "ls", aliases = {"list"}, description = "List all files under a given URI",
    mixinStandardHelpOptions = true)
public class ListFiles extends BaseFileOperation {
  @CommandLine.Option(names = {"-r", "--recursive"}, description = "Recursively list subdirectories")
  private boolean _recursive;

  @CommandLine.Parameters(arity = "1..*", description = "File paths to list")
  private String[] _filePaths;

  public ListFiles setRecursive(boolean recursive) {
    _recursive = recursive;
    return this;
  }

  public ListFiles setFilePaths(String[] filePaths) {
    _filePaths = filePaths;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      super.initialPinotFS();
    } catch (Exception e) {
      System.err.println("Failed to initialize PinotFS, exception: " + e.getMessage());
      return false;
    }

    for (String filePath : _filePaths) {
      URI pathURI = URI.create(filePath);
      String scheme = Utils.getScheme(pathURI);
      if (!PinotFSFactory.isSchemeSupported(scheme)) {
        System.err.println("Scheme: " + scheme + " is not registered.");
        return false;
      }
      try {
        PinotFS pinotFS = PinotFSFactory.create(scheme);
        if (pinotFS.isDirectory(pathURI)) {
          System.out.println(filePath + ":\n");
          System.out.println(StringUtil.join("\n", pinotFS.listFiles(pathURI, _recursive)) + "\n");
        } else if (pinotFS.exists(pathURI)) {
          System.out.println(pathURI + "\n");
        } else {
          System.out.println("ls: " + pathURI + ": No such file or directory");
          return false;
        }
      } catch (IOException e) {
        System.err.println("Unable to list files under: " + pathURI + ", exception: " + e.getMessage());
        return false;
      }
    }
    return true;
  }
}
