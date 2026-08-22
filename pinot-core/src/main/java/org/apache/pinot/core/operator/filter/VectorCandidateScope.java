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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Immutable query-scoped constraint that must be applied before vector candidate selection.
///
/// The factory owns the single defensive copy of the supplied bitmap. Operators can safely share this scope and only
/// allocate another bitmap when they need to intersect it with an independent optional filter.
public final class VectorCandidateScope {
  private final ImmutableRoaringBitmap _requiredDocIds;
  private final String _fallbackReason;

  private VectorCandidateScope(ImmutableRoaringBitmap requiredDocIds, String fallbackReason) {
    byte[] serializedDocIds = new byte[requiredDocIds.serializedSizeInBytes()];
    requiredDocIds.serialize(ByteBuffer.wrap(serializedDocIds));
    _requiredDocIds = new ImmutableRoaringBitmap(ByteBuffer.wrap(serializedDocIds).asReadOnlyBuffer());
    _fallbackReason = Preconditions.checkNotNull(fallbackReason, "Fallback reason must not be null");
  }

  /// Creates a mandatory candidate scope from a detached FULL-upsert valid-document snapshot.
  public static VectorCandidateScope forUpsertSnapshot(ImmutableRoaringBitmap requiredDocIds,
      String fallbackReason) {
    return new VectorCandidateScope(Preconditions.checkNotNull(requiredDocIds, "Required doc IDs must not be null"),
        fallbackReason);
  }

  public ImmutableRoaringBitmap getRequiredDocIds() {
    return _requiredDocIds;
  }

  public String getFallbackReason() {
    return _fallbackReason;
  }
}
