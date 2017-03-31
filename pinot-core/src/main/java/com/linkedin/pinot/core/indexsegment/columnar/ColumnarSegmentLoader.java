/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.indexsegment.columnar;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class ColumnarSegmentLoader {
  private ColumnarSegmentLoader() {
  }

  /**
   * For tests only.
   */
  public static IndexSegment load(@Nonnull File indexDir, @Nonnull ReadMode readMode)
      throws Exception {
    return Loaders.IndexSegment.load(indexDir, readMode);
  }

  /**
   * For segments from REALTIME table.
   */
  public static IndexSegment load(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return Loaders.IndexSegment.load(indexDir, indexLoadingConfig, null);
  }

  /**
   * For segments from OFFLINE table.
   */
  public static IndexSegment load(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema)
      throws Exception {
    return Loaders.IndexSegment.load(indexDir, indexLoadingConfig, schema);
  }
}
