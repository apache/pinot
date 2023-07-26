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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.locationtech.jts.geom.Geometry;


/**
 * Index creator for geospatial index.
 */
public interface GeoSpatialIndexCreator extends IndexCreator {

  Geometry deserialize(byte[] bytes);

  @Override
  default void add(@Nonnull Object value, int dictId)
      throws IOException {
    add(deserialize((byte[]) value));
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds)
      throws IOException {
  }

  /**
   * Adds the next geospatial value.
   */
  void add(Geometry geometry)
      throws IOException;

  /**
   * Seals the index and flushes it to disk.
   */
  void seal()
      throws IOException;
}
