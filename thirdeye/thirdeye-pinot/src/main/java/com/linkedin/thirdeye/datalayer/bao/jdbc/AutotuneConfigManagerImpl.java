/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AutotuneConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.List;


@Singleton
public class AutotuneConfigManagerImpl extends AbstractManagerImpl<AutotuneConfigDTO>
    implements AutotuneConfigManager {

  private final String FUNCTION_ID = "functionId";
  private final String AUTOTUNE_METHOD = "autoTuneMethod";
  private final String PERFORMANCE_EVALUATION_METHOD = "performanceEvaluationMethod";
  private final String START_TIME = "startTime";
  private final String END_TIME = "endTime";
  private final String GOAL = "goal";



  public AutotuneConfigManagerImpl() {
    super(AutotuneConfigDTO.class, AutotuneConfigBean.class);
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFunctionId(long functionId) {
    Predicate predicate = Predicate.EQ(FUNCTION_ID, functionId);
    return findByPredicate(predicate);
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFunctionIdAndAutotuneMethod(long functionId, String autoTuneMethod) {
    Predicate predicate = Predicate.AND( Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod));
    return findByPredicate(predicate);
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFunctionIdAutotuneAndEvaluationMethod(long functionId,
      String autoTuneMethod, String performanceEvaluationMethod) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod), Predicate.EQ(PERFORMANCE_EVALUATION_METHOD, performanceEvaluationMethod));

    return findByPredicate(predicate);
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.GE(START_TIME, startTime), Predicate.LE(END_TIME, endTime));

    return findByPredicate(predicate);
  }
}
