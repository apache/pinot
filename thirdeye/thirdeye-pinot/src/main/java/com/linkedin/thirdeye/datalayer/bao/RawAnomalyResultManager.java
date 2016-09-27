package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public interface RawAnomalyResultManager extends AbstractManager<RawAnomalyResultDTO> {

  List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId);

  List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId);

  List<RawAnomalyResultDTO> findByFunctionId(Long functionId);

}
