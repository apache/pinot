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
package org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * H3 Index creator that uses on-heap memory.
 * <p>On-heap creator uses more heap memory, but is cheaper on computation and does not flush data to disk which can
 * slow down the creation because of the IO latency. Use on-heap creator in the environment where there is enough heap
 * memory and garbage collection won't cause performance issue (e.g. Hadoop/Spark/Pinot Minion).
 */
public class OnHeapH3IndexCreator extends BaseH3IndexCreator {

  public OnHeapH3IndexCreator(File indexDir, String columnName, H3IndexResolution resolution) throws IOException {
    super(indexDir, columnName, resolution);
  }

  @Override
  public void seal() throws IOException {
    for (Map.Entry<Long, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
      add(entry.getKey(), entry.getValue().get());
    }
    generateIndexFile();
  }
}
