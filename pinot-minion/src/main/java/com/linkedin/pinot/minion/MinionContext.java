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

import com.linkedin.pinot.core.minion.SegmentPurger;
import com.linkedin.pinot.minion.metrics.MinionMetrics;
import java.io.File;


/**
 * The <code>MinionContext</code> class is a singleton class which contains all minion related context.
 */
public class MinionContext {
  private static final MinionContext INSTANCE = new MinionContext();

  private MinionContext() {
  }

  public static MinionContext getInstance() {
    return INSTANCE;
  }

  private File _dataDir;
  private MinionMetrics _minionMetrics;
  private String _minionVersion;

  // For PurgeTask
  private SegmentPurger.RecordPurgerFactory _recordPurgerFactory;
  private SegmentPurger.RecordModifierFactory _recordModifierFactory;

  public File getDataDir() {
    return _dataDir;
  }

  public void setDataDir(File dataDir) {
    _dataDir = dataDir;
  }

  public MinionMetrics getMinionMetrics() {
    return _minionMetrics;
  }

  public void setMinionMetrics(MinionMetrics minionMetrics) {
    _minionMetrics = minionMetrics;
  }

  public String getMinionVersion() {
    return _minionVersion;
  }

  public void setMinionVersion(String minionVersion) {
    _minionVersion = minionVersion;
  }

  public SegmentPurger.RecordPurgerFactory getRecordPurgerFactory() {
    return _recordPurgerFactory;
  }

  public void setRecordPurgerFactory(SegmentPurger.RecordPurgerFactory recordPurgerFactory) {
    _recordPurgerFactory = recordPurgerFactory;
  }

  public SegmentPurger.RecordModifierFactory getRecordModifierFactory() {
    return _recordModifierFactory;
  }

  public void setRecordModifierFactory(SegmentPurger.RecordModifierFactory recordModifierFactory) {
    _recordModifierFactory = recordModifierFactory;
  }
}
