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
package org.apache.pinot.tools.admin.command.fs;

import java.io.File;
import java.net.URI;
import org.kohsuke.args4j.Argument;

public class CopyToLocalCommand extends AbstractBasePinotFSCommand {

  @Argument(index = 0, required = true)
  private URI _srcFileUri;

  @Argument(index = 1, required = true)
  private File _dstFileUri;

  @Override
  public boolean execute() throws Exception {
    getPinotFS(_srcFileUri).copyToLocalFile(_srcFileUri, _dstFileUri);
    return true;
  }

  @Override
  public String description() {
    return "Copy files in a Pinot file system to the local file system";
  }

  @Override
  String getSubCommandUsage() {
    return null;
  }
}
