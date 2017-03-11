package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.FunctionAutotuneConfigDTO;
import java.util.List;


public interface FunctionAutotuneConfigManager extends AbstractManager<FunctionAutotuneConfigDTO> {

  List<FunctionAutotuneConfigDTO> findAllByFunctionId(long functionId);

  List<FunctionAutotuneConfigDTO> findAllByFunctionIdAndAutotuneMethod(long functionId, String autoTuneMethod);

  List<FunctionAutotuneConfigDTO> findAllByFunctionIdAutotuneAndEvaluationMethod(long functionId, String autoTuneMethod,
      String performanceEvaluationMethod);

  List<FunctionAutotuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime);

  List<FunctionAutotuneConfigDTO> findAllByFunctionIdWindowGoal(long functionId, long startTime, long endTime,
      double goal);
}
