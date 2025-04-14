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
package org.apache.pinot.spi.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BasePinotFS implements PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePinotFS.class);

  @Override
  public boolean deleteBatch(List<URI> segmentUris, boolean forceDelete)
      throws IOException {
    boolean result = true;
    for (URI segmentUri : segmentUris) {
      result &= delete(segmentUri, forceDelete);
    }
    return result;
  }

  @Override
  public boolean move(URI srcUri, URI dstUri, boolean overwrite)
      throws IOException {
    if (!exists(srcUri)) {
      LOGGER.warn("Source {} does not exist", srcUri);
      return false;
    }
    if (exists(dstUri)) {
      if (overwrite) {
        delete(dstUri, true);
      } else {
        // dst file exists, returning
        LOGGER.warn("Cannot move {} to {}. Destination exists and overwrite flag set to false.", srcUri, dstUri);
        return false;
      }
    } else {
      // ensures the parent path of dst exists.
      try {
        Path parentPath = Paths.get(dstUri.getPath()).getParent();
        URI parentUri = new URI(dstUri.getScheme(), dstUri.getAuthority(), parentPath.toString(), null, null);
        mkdir(parentUri);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
    return doMove(srcUri, dstUri);
  }

  /**
   * Actual move implementation for each PinotFS. It should not be directly called, instead use
   * {@link PinotFS#move(URI, URI, boolean)}.
   */
  protected abstract boolean doMove(URI srcUri, URI dstUri)
      throws IOException;
}
