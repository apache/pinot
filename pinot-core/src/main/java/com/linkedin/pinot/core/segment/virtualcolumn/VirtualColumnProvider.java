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
package com.linkedin.pinot.core.segment.virtualcolumn;

import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;


/**
 * Virtual column provider interface, which is used to instantiate the various components (dictionary, reader, etc) that
 * comprise a proper column.
 */
public interface VirtualColumnProvider {
  DataFileReader buildReader(VirtualColumnContext context);
  Dictionary buildDictionary(VirtualColumnContext context);
  ColumnMetadata buildMetadata(VirtualColumnContext context);
  InvertedIndexReader buildInvertedIndex(VirtualColumnContext context);
  ColumnIndexContainer buildColumnIndexContainer(VirtualColumnContext context);
}
