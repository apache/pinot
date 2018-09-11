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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;


/**
 * Factory for virtual column providers.
 */
public class VirtualColumnProviderFactory {
  public static VirtualColumnProvider buildProvider(String virtualColumnProvider) {
    try {
      Class<? extends VirtualColumnProvider> providerClass =
          (Class<? extends VirtualColumnProvider>) Class.forName(virtualColumnProvider);
      return providerClass.newInstance();
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  public static void addBuiltInVirtualColumnsToSchema(Schema schema) {
    if (!schema.hasColumn("$docId")) {
      schema.addField(new DimensionFieldSpec("$docId", FieldSpec.DataType.INT, true, DocIdVirtualColumnProvider.class));
    }

    if (!schema.hasColumn("$hostName")) {
      schema.addField(new DimensionFieldSpec("$hostName", FieldSpec.DataType.STRING, true, HostNameVirtualColumnProvider.class));
    }

    if (!schema.hasColumn("$segmentName")) {
      schema.addField(new DimensionFieldSpec("$segmentName", FieldSpec.DataType.STRING, true, SegmentNameVirtualColumnProvider.class));
    }
  }
}
