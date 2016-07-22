package com.linkedin.thirdeye.detector.function;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.detector.db.entity.AnomalyResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAnomalyFunction implements AnomalyFunction {
  private AnomalyFunctionSpec spec;
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  static final Joiner CSV = Joiner.on(",");

  // 5 minutes
  static long DEFAULT_MERGE_TIME_DELTA_MILLIS = 5 * 60 * 1000;

  @Override
  public void init(AnomalyFunctionSpec spec) throws Exception {
    this.spec = spec;
  }

  @Override
  public AnomalyFunctionSpec getSpec() {
    return spec;
  }

  protected Properties getProperties() throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }

  /**
   * Returns unit change from baseline value
   * @param currentValue
   * @param baselineValue
   * @return
   */
  protected double calculateChange(double currentValue, double baselineValue) {
    return (currentValue - baselineValue) / baselineValue;
  }

  /**
   * Scans and merges consecutive results (when timeDiff between results are lesser than timeDeltaMillies)
   * @param results : results
   * @param timeDeltaMillis : allowed time difference for merging of results
   * @return
   */
  List<AnomalyResult> mergeResults(List<AnomalyResult> results, long timeDeltaMillis) {
    // sorting on start time in natural order
    Collections.sort(results, new Comparator<AnomalyResult>() {
      @Override public int compare(AnomalyResult o1, AnomalyResult o2) {
        return (int) ((o1.getStartTimeUtc() - o2.getStartTimeUtc())/1000);
      }
    });
    if (results.size() > 1) {
      List<AnomalyResult> mergedResults = new ArrayList<>();
      AnomalyResult prevResult = results.get(0);
      for (int i = 1; i < results.size(); i++) {
        AnomalyResult currentResult = results.get(i);
        if (currentResult.getStartTimeUtc() > (prevResult.getEndTimeUtc() + timeDeltaMillis)) {
          mergedResults.add(prevResult);
          prevResult = currentResult;
        } else {
          // Merge current result with the previous one
          prevResult.setEndTimeUtc(currentResult.getEndTimeUtc());
          // update the score to highest value
          if (currentResult.getScore() > prevResult.getScore()) {
            prevResult.setScore(currentResult.getScore());
            prevResult.setMessage(currentResult.getMessage());
          }
        }
      }
      mergedResults.add(prevResult);
      LOG.info("Merged {} anomaly results to {}", results.size(), mergedResults.size());
      return mergedResults;
    } else {
      return results;
    }
  }
}
