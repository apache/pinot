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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for AVRO file.
 */
public class TestRecordReader implements RecordReader {

  List<GenericRow> _rows = new ArrayList<>();
  Iterator<GenericRow> _iterator;

  public void init(File dataFile, Set<String> fields, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    int numRows = 10;
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue("key", "value-" + i);
      _rows.add(row);
    }
    _iterator = _rows.iterator();
  }

  public boolean hasNext() {
    return _iterator.hasNext();
  }

  public GenericRow next()
      throws IOException {
    return _iterator.next();
  }

  public GenericRow next(GenericRow reuse)
      throws IOException {
    return _iterator.next();
  }

  public void rewind()
      throws IOException {
    _iterator = _rows.iterator();
  }

  public void close() {

  }
}