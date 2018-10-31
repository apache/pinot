package com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;

public class DummyAlertGroupAuxiliaryInfoProvider extends BaseAlertGroupAuxiliaryInfoProvider {
  @Override
  public AuxiliaryAlertGroupInfo getAlertGroupAuxiliaryInfo(DimensionMap dimensions, List<MergedAnomalyResultDTO> anomalyResultList) {
    return EMPTY_AUXILIARY_ALERT_GROUP_INFO;
  }
}
