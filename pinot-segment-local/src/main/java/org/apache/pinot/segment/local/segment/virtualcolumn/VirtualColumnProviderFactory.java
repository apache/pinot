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

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.spi.data.BuiltInVirtualColumnDefinitions;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.NetUtils;


/// Factory for virtual column providers.
public class VirtualColumnProviderFactory {
  /// Provider for each built-in virtual column. Its key set is asserted to cover
  /// [BuiltInVirtualColumnDefinitions#DEFINITIONS] by `SegmentMetadataVirtualColumnProviderTest`, so a column added
  /// there without a provider here is caught at build time; at runtime `getProviderClass` names the offending column.
  private static final Map<String, Class<? extends VirtualColumnProvider>> PROVIDER_CLASSES =
      Map.of(BuiltInVirtualColumn.DOCID, DocIdVirtualColumnProvider.class,
          BuiltInVirtualColumn.HOSTNAME, DefaultNullValueVirtualColumnProvider.class,
          BuiltInVirtualColumn.SEGMENTNAME, DefaultNullValueVirtualColumnProvider.class,
          BuiltInVirtualColumn.PARTITIONID, PartitionIdVirtualColumnProvider.class,
          BuiltInVirtualColumn.CREATIONTIME, SegmentCreationTimeVirtualColumnProvider.class,
          BuiltInVirtualColumn.STARTTIME, SegmentStartTimeVirtualColumnProvider.class,
          BuiltInVirtualColumn.ENDTIME, SegmentEndTimeVirtualColumnProvider.class,
          BuiltInVirtualColumn.TOTALDOCS, SegmentTotalDocsVirtualColumnProvider.class,
          BuiltInVirtualColumn.CRC, SegmentCrcVirtualColumnProvider.class);

  /// Shared instances of the built-in providers, keyed by the class name stored in the field spec.
  private static final Map<String, VirtualColumnProvider> BUILT_IN_PROVIDERS = PROVIDER_CLASSES.values()
      .stream()
      .distinct()
      .collect(Collectors.toUnmodifiableMap(Class::getName, VirtualColumnProviderFactory::newInstance));

  private VirtualColumnProviderFactory() {
  }

  private static VirtualColumnProvider newInstance(Class<? extends VirtualColumnProvider> providerClass) {
    try {
      return providerClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while creating instance of: " + providerClass.getName(), e);
    }
  }

  public static VirtualColumnProvider buildProvider(VirtualColumnContext virtualColumnContext) {
    String virtualColumnProvider = virtualColumnContext.getFieldSpec().getVirtualColumnProvider();
    // The built-in providers are stateless - everything they need comes from the VirtualColumnContext - so a single
    // shared instance serves every column of every segment. This matters at segment load, which resolves a provider
    // per virtual column per segment, and on the mutable path, which resolves one per query.
    VirtualColumnProvider builtInProvider = BUILT_IN_PROVIDERS.get(virtualColumnProvider);
    if (builtInProvider != null) {
      return builtInProvider;
    }
    try {
      return PluginManager.get().createInstance(virtualColumnProvider);
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while creating instance of: " + virtualColumnProvider, e);
    }
  }

  /// Adds the built-in virtual columns to the schema of a segment, together with the provider that produces their
  /// values.
  ///
  /// The shape of each column (name, data type, single-value vs multi-value) comes from
  /// [BuiltInVirtualColumnDefinitions#DEFINITIONS], which the broker side uses as well, so the two can never
  /// disagree on a type.
  /// This method only layers on the provider class, and the constant value for the columns whose value is already
  /// known here.
  public static void addBuiltInVirtualColumnsToSegmentSchema(Schema schema, String segmentName) {
    for (BuiltInVirtualColumnDefinitions.Definition definition : BuiltInVirtualColumnDefinitions.DEFINITIONS) {
      String column = definition.getName();
      if (schema.hasColumn(column)) {
        continue;
      }
      DimensionFieldSpec fieldSpec = definition.createFieldSpec();
      fieldSpec.setVirtualColumnProvider(getProviderClass(column).getName());
      // $hostName and $segmentName are constants known at schema construction time, and are carried as the field's
      // default null value, which DefaultNullValueVirtualColumnProvider reads back.
      if (BuiltInVirtualColumn.HOSTNAME.equals(column)) {
        fieldSpec.setDefaultNullValue(NetUtils.getHostnameOrAddress());
      } else if (BuiltInVirtualColumn.SEGMENTNAME.equals(column)) {
        fieldSpec.setDefaultNullValue(segmentName);
      }
      schema.addField(fieldSpec);
    }
  }

  private static Class<? extends VirtualColumnProvider> getProviderClass(String column) {
    Class<? extends VirtualColumnProvider> providerClass = PROVIDER_CLASSES.get(column);
    Preconditions.checkState(providerClass != null, "No virtual column provider registered for built-in column: %s",
        column);
    return providerClass;
  }
}
