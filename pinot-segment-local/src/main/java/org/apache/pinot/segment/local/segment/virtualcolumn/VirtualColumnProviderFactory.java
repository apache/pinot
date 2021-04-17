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

import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.NetUtils;


/**
 * Factory for virtual column providers.
 */
public class VirtualColumnProviderFactory {
  public static VirtualColumnProvider buildProvider(VirtualColumnContext virtualColumnContext) {
    String virtualColumnProvider = virtualColumnContext.getFieldSpec().getVirtualColumnProvider();
    try {
      // Use the preset virtualColumnProvider if available
      if (virtualColumnProvider != null
          && !virtualColumnProvider.equals(DefaultNullValueVirtualColumnProvider.class.getName())) {
        return PluginManager.get().createInstance(virtualColumnProvider);
      }
      // Create the columnProvider that returns default null values based on the virtualColumnContext
      return new DefaultNullValueVirtualColumnProvider();
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while creating instance of: " + virtualColumnProvider, e);
    }
  }

  public static void addBuiltInVirtualColumnsToSegmentSchema(Schema schema, String segmentName) {
    if (!schema.hasColumn(BuiltInVirtualColumn.DOCID)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.DOCID, FieldSpec.DataType.INT, true,
          DocIdVirtualColumnProvider.class));
    }

    if (!schema.hasColumn(BuiltInVirtualColumn.HOSTNAME)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.HOSTNAME, FieldSpec.DataType.STRING, true,
          DefaultNullValueVirtualColumnProvider.class, NetUtils.getHostnameOrAddress()));
    }

    if (!schema.hasColumn(BuiltInVirtualColumn.SEGMENTNAME)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.SEGMENTNAME, FieldSpec.DataType.STRING, true,
          DefaultNullValueVirtualColumnProvider.class, segmentName));
    }
  }
}
