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
package org.apache.pinot.segment.local.indexsegment;

import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;


public class IndexSegmentUtils {
  private IndexSegmentUtils() {
  }

  /// Returns a virtual [DataSource] per the given [VirtualColumnContext].
  public static DataSource createVirtualDataSource(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    VirtualColumnProvider virtualColumnProvider;
    if (fieldSpec.getVirtualColumnProvider() != null) {
      virtualColumnProvider = VirtualColumnProviderFactory.buildProvider(context);
    } else {
      virtualColumnProvider = new DefaultNullValueVirtualColumnProvider();
    }
    return virtualColumnProvider.buildDataSource(context);
  }
}
