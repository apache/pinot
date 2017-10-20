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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.Converter;
import com.linkedin.pinot.core.minion.RawIndexConverter;
import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConvertToRawIndexTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConvertToRawIndexTaskExecutor.class);

  @Override
  public String getTaskType() {
    return MinionConstants.ConvertToRawIndexTask.TASK_TYPE;
  }

  @Override
  public Converter getConverter(File indexDir, File convertedIndexDir, Map<String, String> configs) throws Exception {
    return new RawIndexConverter(indexDir, convertedIndexDir, configs.get(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY));
  }
}
