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
package org.apache.pinot.core.segment.creator.impl.presence;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Presence Vector Creator
 *
 */
public class PresenceVectorCreator implements Closeable {

  private MutableRoaringBitmap _nullBitmap;
  private File _presenceVectorFile;

  public PresenceVectorCreator(File indexDir, String columnName) {
    _presenceVectorFile = new File(indexDir, columnName + V1Constants.Indexes.PRESENCE_VECTOR_FILE_EXTENSION);
  }

  @Override
  public void close()
      throws IOException {
    try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(_presenceVectorFile))) {
      _nullBitmap.serialize(outputStream);
    }
  }

  public void setIsNull(int docId) {
    _nullBitmap.add(docId);
  }


  protected ImmutableBitmapDataProvider getRoaringBitmap() {
    return _nullBitmap;
  }
}
