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
import javax.annotation.Nullable;


/**
 * Tracks the transformation of a derived object to its creator object.
 *
 * This is useful for tracking the lineage of objects in a transformation pipeline.
 * For example, multi-stage engine uses trackers to be able to replace original logical nodes in a plan with the
 * {@link org.apache.pinot.core.plan.PinotExplainedRelNode} nodes returned by each server.
 *
 * These trackers assume that the creation process is deterministic and that a derived object is always created by a
 * single creator object. On the contrary, a creator object can create multiple derived objects.
 *
 * Trackers are, in deep, very similar to {@link java.util.Map#get(Object)} on a map from derived objects to creator
 * objects.
 * However, they provide a higher-level interface that allows for more expressive and easier to read transformations.
 *
 * @param <D> Derived object. This is the object that is created by a transformation.
 * @param <C> Creator object. This is the object that creates the derived object.
 */
public interface TransformationTracker<D, C> {
  /**
   * Returns the creator object of the derived object.
   */
  @Nullable
  C getCreatorOf(D derived);

  /**
   * Builder for {@link TransformationTracker}.
   *
   * It is expected for transformation methods to receive a builder and add the relations between derived and creator
   * objects. While the transformation is being executed, the builder can be used as a tracker.
   *
   * While a {@link TransformationTracker} can be seen as the {@link java.util.Map#get(Object)} method of a map from
   * derived objects to creator objects, the builder can be seen as the {@link java.util.Map#put(Object, Object)}.
   */
  interface Builder<D, C> extends TransformationTracker<D, C> {
    Builder<D, C> trackCreation(C creator, D derived);

    TransformationTracker<D, C> build();
  }

  /**
   * Tracks the transformation of a derived object to its creator object by identity.
   *
   * This means that the tracker does not use the equals method of the derived object to track the creator object but
   * instead compares the references of the derived object.
   */
  class ByIdentity<D, C> implements TransformationTracker<D, C> {
    private final IdentityHashMap<D, C> _directMap;

    public ByIdentity(IdentityHashMap<D, C> directMap) {
      _directMap = directMap;
    }

    @Override
    public C getCreatorOf(D derived) {
      return _directMap.get(derived);
    }

    public static class Builder<D, C> implements TransformationTracker.Builder<D, C> {
      private final TransformationTracker.ByIdentity<D, C> _partial
          = new TransformationTracker.ByIdentity<>(new IdentityHashMap<>());
      private boolean _built = false;

      @Override
      public C getCreatorOf(D derived) {
        return _partial.getCreatorOf(derived);
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
