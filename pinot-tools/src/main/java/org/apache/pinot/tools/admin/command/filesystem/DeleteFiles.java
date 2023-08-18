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
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "rm", aliases = {"delete", "del"}, description = "Delete files",
    mixinStandardHelpOptions = true)
public class DeleteFiles extends BaseFileOperation {
  @CommandLine.Option(names = {"-f", "--force"}, description = "Force delete if possible")
  private boolean _force;

  @CommandLine.Parameters(arity = "1..*", description = "File paths to delete")
  private String[] _filePaths;

  public DeleteFiles setForce(boolean force) {
    _force = force;
    return this;
  }

  public DeleteFiles setFilePaths(String[] filePaths) {
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
        PinotFSFactory.create(scheme).delete(pathURI, _force);
      } catch (IOException e) {
        System.err.println("Unable to delete files under: " + pathURI + ", exception: " + e.getMessage());
        return false;
      }
    }
    return true;
  }

//  @Override
//  public void getDescription() {
//    System.out.println("rm [-f] <path-uri>...");
//  }
}
