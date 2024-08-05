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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.IdentityHashMap;


public interface TransformationTracker<D, C> {
  C getCreatorOf(D derived);

  boolean isTracked(D derived);

  interface Builder<D, C> extends TransformationTracker<D, C> {
    Builder<D, C> trackCreation(C creator, D derived);
  }

  class ByIdentity<D, C> implements TransformationTracker<D, C> {
    private final IdentityHashMap<D, C> _directMap;

    public ByIdentity(IdentityHashMap<D, C> directMap) {
      _directMap = directMap;
    }

    @Override
    public C getCreatorOf(D derived) {
      return _directMap.get(derived);
    }

    @Override
    public boolean isTracked(D derived) {
      return _directMap.containsKey(derived);
    }

    public static class Builder<D, C> implements TransformationTracker.Builder<D, C> {
      private final TransformationTracker.ByIdentity<D, C> _partial
          = new TransformationTracker.ByIdentity<>(new IdentityHashMap<>());
      private boolean _built = false;

      @Override
      public C getCreatorOf(D derived) {
        return _partial.getCreatorOf(derived);
      }

      @Override
      public boolean isTracked(D derived) {
        return _partial.isTracked(derived);
      }

      public TransformationTracker.Builder<D, C> trackCreation(C creator, D derived) {
        Preconditions.checkNotNull(derived, "derived cannot be null");
        Preconditions.checkNotNull(creator, "creator cannot be null");
        Preconditions.checkState(!_built, "Cannot add more relations after building");
        _partial._directMap.put(derived, creator);
        return this;
      }

      public ByIdentity<D, C> build() {
        _built = true;
        return _partial;
      }
    }
  }
}
