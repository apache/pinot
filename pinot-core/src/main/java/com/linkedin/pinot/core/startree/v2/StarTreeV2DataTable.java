/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.startree.v2;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;


public class StarTreeV2DataTable implements Closeable {

  private final PinotDataBuffer _dataBuffer;
  private final int _dimensionSize;
  private final int _metricSize;
  private final int _docSize;
  private final long _docSizeLong;
  private final int _startDocId;

  /**
   * Constructor of the StarTreeDataTable.
   *
   * @param dataBuffer Data buffer
   * @param dimensionSize Size of all dimensions in bytes
   * @param metricSize Size of all metrics in bytes
   * @param startDocId Start document id of the data buffer
   */
  public StarTreeV2DataTable(PinotDataBuffer dataBuffer, int dimensionSize, int metricSize, int startDocId) {
    Preconditions.checkState(dataBuffer.size() > 0);

    _dataBuffer = dataBuffer;
    _dimensionSize = dimensionSize;
    _metricSize = metricSize;
    _docSize = dimensionSize + metricSize;
    _docSizeLong = _docSize;
    _startDocId = startDocId;
  }

  @Override
  public void close() throws IOException {

  }
}
