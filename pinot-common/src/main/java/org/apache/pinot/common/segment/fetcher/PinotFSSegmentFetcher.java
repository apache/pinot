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
package org.apache.pinot.common.segment.fetcher;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.filesystem.PinotFS;
import org.apache.pinot.filesystem.PinotFSFactory;


public class PinotFSSegmentFetcher implements SegmentFetcher {
  private PinotFS _pinotFS;

  @Override
  public void init(Configuration config) {

  }

  @Override
  public void fetchSegmentToLocal(String uriString, File tempFile)
      throws Exception {
    URI uri = new URI(uriString);

    // TODO: move _pinotFS creation to init, however, it needs the right config passed in into init.
    _pinotFS = PinotFSFactory.create(uri.getScheme());
    _pinotFS.copyToLocalFile(uri, tempFile);
  }

  /**
   * Returns a list of config keys whose value should not be logged.
   *
   * @return List of protected config keys
   */
  @Override
  public Set<String> getProtectedConfigKeys() {
    return Collections.emptySet();
  }
}
