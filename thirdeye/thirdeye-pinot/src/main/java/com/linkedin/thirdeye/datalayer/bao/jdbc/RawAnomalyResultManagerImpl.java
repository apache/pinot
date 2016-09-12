package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.List;

import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;

public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO> implements RawAnomalyResultManager {

  public RawAnomalyResultManagerImpl() {
    super(RawAnomalyResultDTO.class, RawAnomalyResultBean.class);
  }


  public List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    return null;
  }

  public List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId) {
    return null;
  }
}
