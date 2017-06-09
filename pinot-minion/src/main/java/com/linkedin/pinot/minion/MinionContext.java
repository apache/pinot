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
package com.linkedin.pinot.minion;

import com.linkedin.pinot.minion.metrics.MinionMetrics;
import java.io.File;
import javax.annotation.Nonnull;


/**
 * The <code>MinionContext</code> class contains all minion related context.
 */
public class MinionContext {
  private final File _dataDir;
  private final MinionMetrics _minionMetrics;

  public MinionContext(@Nonnull File dataDir, @Nonnull MinionMetrics minionMetrics) {
    _dataDir = dataDir;
    _minionMetrics = minionMetrics;
  }

  public File getDataDir() {
    return _dataDir;
  }

  public MinionMetrics getMinionMetrics() {
    return _minionMetrics;
  }
}
