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
package org.apache.pinot.core.data.manager.offline;

import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.data.Schema;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/// Binds native installation provenance and a detached loading schema to the exact reference-counted segment handle.
/// Replacing a segment name cannot change the identity or schema held by an already admitted reader. Construction
/// alone grants no serving authority; query admission must also validate current permits and the expected identity.
public final class NativeImmutableSegmentDataManager extends ImmutableSegmentDataManager {
  private final Identity _installationIdentity;
  private final Schema _querySchema;

  public NativeImmutableSegmentDataManager(ImmutableSegment segment, Identity identity, Schema querySchema) {
    super(requireNonNull(segment, "segment"));
    _installationIdentity = requireNonNull(identity, "identity");
    requireNonNull(querySchema, "querySchema");
    checkArgument(segment.getSegmentName().equals(identity.segmentName()), "Native segment identity differs");
    checkArgument(querySchema.getSchemaName().equals(identity.schemaName()), "Native schema identity differs");
    _querySchema = Schema.cloneSchemaWithName(querySchema, querySchema.getSchemaName());
  }

  public Identity getInstallationIdentity() {
    return _installationIdentity;
  }

  /// Returns a detached schema so query-local normalization cannot change another reader's pinned schema.
  public Schema getQuerySchema() {
    return Schema.cloneSchemaWithName(_querySchema, _querySchema.getSchemaName());
  }
}
