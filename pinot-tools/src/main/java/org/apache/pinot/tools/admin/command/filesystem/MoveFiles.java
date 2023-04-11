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
import picocli.CommandLine;


@CommandLine.Command(name = "mv", aliases = {"move"}, description = "Move file from source to destination, moving "
                                                                    + "from two different PinotFS is not supported.",
    mixinStandardHelpOptions = true)
public class MoveFiles extends BaseFileOperation {
  @CommandLine.Option(names = {"-o", "--overwrite"}, description = "Overwrite if destination already exists")
  private boolean _overwrite;

  @CommandLine.Parameters(index = "0", description = "Source file")
  private String _source;

  @CommandLine.Parameters(index = "1", description = "Destination file")
  private String _destination;

  public MoveFiles setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
    return this;
  }

  public MoveFiles setSource(String source) {
    _source = source;
    return this;
  }

  public MoveFiles setDestination(String destination) {
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
        sourcePinotFS.move(sourceURI, destinationURI, _overwrite);
        return true;
      } catch (IOException e) {
        System.err.println(
            "Unable to move files from " + _source + " to " + _destination + ", exception: " + e.getMessage());
      }
    } else {
      System.err.println("Moving files between two different PinotFS is not supported");
    }
    return false;
  }

//  @Override
//  public void getDescription() {
//    System.out.println("mv [-o] <source-uri> <destination-uri>");
//  }
}
