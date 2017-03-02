package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dto.FunctionAutoTuneConfigDTO;
import java.util.List;


public interface FunctionAutoTuneConfigManager extends AbstractManager<FunctionAutoTuneConfigDTO> {

  List<FunctionAutoTuneConfigDTO> findAllByFunctionId(long functionId);

  List<FunctionAutoTuneConfigDTO> findAllByFunctionIdAndAutoTuneMethod(long functionId, String autoTuneMethod);

  List<FunctionAutoTuneConfigDTO> findAllByFunctionIdAutoTuneAndEvaluationMethod(long functionId, String autoTuneMethod,
      String performanceEvaluationMethod);

  List<FunctionAutoTuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime);
}
