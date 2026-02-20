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
package org.apache.pinot.core.data.manager;

/**
 * Labels the task type for segment operations throttler binding.
 *
 * <p>This is implemented as a class with static instances (type-safe enum pattern) rather than a Java enum
 * to allow extensibility. Extend this class to define additional custom task types while maintaining compatibility
 * with the base task types defined here.</p>
 */
public class SegmentOperationsTaskType {
  public static final SegmentOperationsTaskType CONSUMER =
      new SegmentOperationsTaskType("CONSUMER");
  public static final SegmentOperationsTaskType STATE_TRANSITION =
      new SegmentOperationsTaskType("STATE_TRANSITION");
  public static final SegmentOperationsTaskType REFRESH_OR_RELOAD =
      new SegmentOperationsTaskType("REFRESH_OR_RELOAD");
  public static final SegmentOperationsTaskType PRELOAD =
      new SegmentOperationsTaskType("PRELOAD");

  private final String _name;

  protected SegmentOperationsTaskType(String name) {
    _name = name;
  }

  @Override
  public String toString() {
    return _name;
  }

  @Override
  public int hashCode() {
    return _name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SegmentOperationsTaskType other = (SegmentOperationsTaskType) obj;
    return _name.equals(other._name);
  }
}
