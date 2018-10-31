package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import java.util.List;


public interface AutotuneConfigManager extends AbstractManager<AutotuneConfigDTO> {

  List<AutotuneConfigDTO> findAllByFunctionId(long functionId);

  List<AutotuneConfigDTO> findAllByFunctionIdAndAutotuneMethod(long functionId, String autoTuneMethod);

  List<AutotuneConfigDTO> findAllByFunctionIdAutotuneAndEvaluationMethod(long functionId, String autoTuneMethod,
      String performanceEvaluationMethod);

  List<AutotuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime);
}
