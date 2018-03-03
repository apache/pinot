package com.linkedin.thirdeye.datalayer.bao;


import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;

public interface DetectionStatusManager extends AbstractManager<DetectionStatusDTO>{

  DetectionStatusDTO findLatestEntryForFunctionId(long functionId);
  List<DetectionStatusDTO> findAllInTimeRangeForFunctionAndDetectionRun(long startTime, long endTime, long functionId,
      boolean detectionRun);
  int deleteRecordsOlderThanDays(int days);

}
