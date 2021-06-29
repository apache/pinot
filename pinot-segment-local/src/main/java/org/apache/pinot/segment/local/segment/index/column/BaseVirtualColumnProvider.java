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
package org.apache.pinot.segment.local.segment.index.column;

import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnIndexContainer;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;


/**
 * Shared implementation code between column providers.
 */
public abstract class BaseVirtualColumnProvider implements VirtualColumnProvider {

  protected ColumnMetadataImpl.Builder getColumnMetadataBuilder(VirtualColumnContext context) {
    return new ColumnMetadataImpl.Builder().setFieldSpec(context.getFieldSpec())
        .setTotalDocs(context.getTotalDocCount());
  }

  @Override
  public ColumnIndexContainer buildColumnIndexContainer(VirtualColumnContext context) {
    return new VirtualColumnIndexContainer(buildForwardIndex(context), buildInvertedIndex(context),
        buildDictionary(context));
  }
}
