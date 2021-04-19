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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import org.apache.pinot.segment.local.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;


/**
 * Virtual column provider interface, which is used to instantiate the various components (dictionary, reader, etc) that
 * comprise a proper column.
 */
public interface VirtualColumnProvider {
  ForwardIndexReader<?> buildForwardIndex(VirtualColumnContext context);

  Dictionary buildDictionary(VirtualColumnContext context);

  ColumnMetadata buildMetadata(VirtualColumnContext context);

  InvertedIndexReader<?> buildInvertedIndex(VirtualColumnContext context);

  ColumnIndexContainer buildColumnIndexContainer(VirtualColumnContext context);
}
