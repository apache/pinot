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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import picocli.CommandLine;


@CommandLine.Command(name = "cp", aliases = {"copy"}, description = "Copy file from source to destination, copying "
                                                                    + "from two different remote PinotFS is not "
                                                                    + "supported.", mixinStandardHelpOptions = true)
public class CopyFiles extends BaseFileOperation {
  @CommandLine.Parameters(index = "0", description = "Source file")
  private String _source;

  @CommandLine.Parameters(index = "1", description = "Destination file")
  private String _destination;

  public CopyFiles setSource(String source) {
    _source = source;
    return this;
  }

  public CopyFiles setDestination(String destination) {
    _destination = destination;
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

    URI sourceURI = URI.create(_source);
    PinotFS sourcePinotFS = Utils.getPinotFS(sourceURI);

    URI destinationURI = URI.create(_destination);
    PinotFS destinationPinotFS = Utils.getPinotFS(destinationURI);

    if (sourcePinotFS == destinationPinotFS) {
      try {
        sourcePinotFS.copy(sourceURI, destinationURI);
        return true;
      } catch (IOException e) {
        System.err.println(
            "Unable to copy files from " + _source + " to " + _destination + ", exception: " + e.getMessage());
      }
    } else if (sourcePinotFS instanceof LocalPinotFS) {
      File localFile = new File(_source);
      if (localFile.isDirectory()) {
        try {
          destinationPinotFS.copyFromLocalDir(localFile, destinationURI);
          return true;
        } catch (Exception e) {
          System.err.println(
              "Failed to copy local directory " + _source + " to " + _destination + ", exception: " + e.getMessage());
        }
      } else {
        try {
          destinationPinotFS.copyFromLocalFile(localFile, destinationURI);
          return true;
        } catch (Exception e) {
          System.err.println(
              "Failed to copy local file " + _source + " to " + _destination + ", exception: " + e.getMessage());
        }
      }
    } else if (destinationPinotFS instanceof LocalPinotFS) {
      try {
        sourcePinotFS.copyToLocalFile(sourceURI, new File(_destination));
        return true;
      } catch (Exception e) {
        System.err.println(
            "Failed to copy remote " + _source + " to local file " + _destination + ", exception: " + e.getMessage());
      }
    } else {
      System.err.println("Copying files between two different remote PinotFS is not supported");
    }
    return false;
  }

//  @Override
//  public void getDescription() {
//    System.out.println("cp <source-uri> <destination-uri>");
//  }
}
