package com.linkedin.thirdeye.detection.finetune;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.List;


public interface ScoreFunction {
  double calculateScore(DetectionPipelineResult detectionResult, List<MergedAnomalyResultDTO> testAnomalies);
}
