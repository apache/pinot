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
package org.apache.pinot.segment.local.segment.loader;

import java.net.URI;
import org.apache.pinot.segment.local.loader.DefaultSegmentDirectoryLoader;
import org.apache.pinot.segment.local.loader.TierBasedSegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentDirectoryLoaderRegistryTest {

  @Test
  public void testSegmentDirectoryLoaderRegistry() {
    SegmentDirectoryLoader defaultSegmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader();
    Assert.assertEquals(defaultSegmentDirectoryLoader.getClass().getSimpleName(),
        DefaultSegmentDirectoryLoader.class.getSimpleName());
    defaultSegmentDirectoryLoader = SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader("default");
    Assert.assertEquals(defaultSegmentDirectoryLoader.getClass().getSimpleName(),
        DefaultSegmentDirectoryLoader.class.getSimpleName());

    SegmentDirectoryLoader tierBasedSegmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader("tierBased");
    Assert.assertEquals(tierBasedSegmentDirectoryLoader.getClass().getSimpleName(),
        TierBasedSegmentDirectoryLoader.class.getSimpleName());

    Assert.assertNull(SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader("custom"));
    SegmentDirectoryLoaderRegistry.setSegmentDirectoryLoader("custom", new CustomSegmentDirectoryLoader());
    Assert.assertEquals(SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader("custom").getClass().getSimpleName(),
        CustomSegmentDirectoryLoader.class.getSimpleName());
  }

  private static class CustomSegmentDirectoryLoader implements SegmentDirectoryLoader {

    @Override
    public SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext segmentDirectoryLoaderContext)
        throws Exception {
      return null;
    }
  }
}
