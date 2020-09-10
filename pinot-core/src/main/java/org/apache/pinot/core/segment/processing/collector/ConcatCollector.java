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
package org.apache.pinot.core.segment.processing.collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A Collector implementation for collecting and concatenating all incoming rows
 */
public class ConcatCollector implements Collector {
  private final List<GenericRow> _collection = new ArrayList<>();
  private final GenericRowSorter _sorter;

  public ConcatCollector(CollectorConfig collectorConfig, Schema schema) {
    List<String> sortOrder = collectorConfig.getSortOrder();
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      _sorter = new GenericRowSorter(sortOrder, schema);
    } else {
      _sorter = null;
    }
  }

  @Override
  public void collect(GenericRow genericRow) {
    _collection.add(genericRow);
  }

  @Override
  public Iterator<GenericRow> iterator() {
    if (_sorter != null) {
      _sorter.sort(_collection);
    }
    return _collection.iterator();
  }

  @Override
  public int size() {
    return _collection.size();
  }

  @Override
  public void reset() {
    _collection.clear();
  }
}
