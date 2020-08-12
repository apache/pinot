package org.apache.pinot.thirdeye.datalayer.bao;

import org.apache.pinot.thirdeye.datalayer.dto.OnlineDetectionDataDTO;

import java.util.List;

public interface OnlineDetectionDataManager extends AbstractManager<OnlineDetectionDataDTO>{
  List<OnlineDetectionDataDTO> findByDatasetAndMetric(String dataset, String metric);
}
