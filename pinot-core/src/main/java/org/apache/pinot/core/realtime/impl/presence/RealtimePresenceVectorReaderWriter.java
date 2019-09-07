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
package org.apache.pinot.core.realtime.impl.presence;

import org.apache.pinot.core.segment.index.readers.PresenceVectorReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Defines a real-time presence vector to be used in realtime ingestion.
 */
public class RealtimePresenceVectorReaderWriter implements PresenceVectorReader {
    private final MutableRoaringBitmap _nullBitmap;

    public RealtimePresenceVectorReaderWriter() {
        _nullBitmap = new MutableRoaringBitmap();
    }

    public void setNull(int docId) {
        _nullBitmap.add(docId);
    }

    public boolean isPresent(int docId) {
        return !_nullBitmap.contains(docId);
    }
}
