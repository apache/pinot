/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.utils;

import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.cookie.BasicClientCookie;


/**
 * Utility classes for calling detector endpoints to execute/schedule jobs
 */
public class DetectionResourceHttpUtils extends AbstractResourceHttpUtils {

  private static final String DETECTION_JOB_ENDPOINT = "/api/detection-job/";
  private static final String ADHOC = "/ad-hoc";
  private static final String BACKFILL = "/replay";
  private static final String AUTOTUNE_FILTER = "autotune/filter/";
  private static final String EVAL_FILTER = "eval/filter/";
  private static final String EVAL_AUTOTUNE = "eval/autotune/";
  private static final String CHECK_HAS_LABELS = "initautotune/checkhaslabel/";
  private static final String INIT_AUTOTUNE = "initautotune/filter/";
  private static final String REPLAY_FUNCTION = "replay/function/";

  public DetectionResourceHttpUtils(String detectionHost, int detectionPort, String authToken) {
    super(new HttpHost(detectionHost, detectionPort));
    addAuthenticationCookie(authToken);
  }

  public String enableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(DETECTION_JOB_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String disableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(DETECTION_JOB_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String runAdhocAnomalyFunction(String id, String startTimeIso, String endTimeIso)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + id + ADHOC + "?start=" + startTimeIso + "&end=" + endTimeIso);
    return callJobEndpoint(req);
  }

  public String runBackfillAnomalyFunction(String id, String startTimeIso, String endTimeIso, boolean forceBackfill)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + id + BACKFILL + "?start=" + startTimeIso + "&end=" + endTimeIso + "&force=" + forceBackfill);
    return callJobEndpoint(req);
  }

  public String runAutoTune(Long functionId, String startTimeIso, String endTimeIso, String autoTuneType, String holidayStarts, String holidayEnds) throws Exception {
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + AUTOTUNE_FILTER + functionId
            + "?start=" + startTimeIso
        + "&end=" + endTimeIso
        + "&autoTuneType=" + autoTuneType
        + "&holidayStarts=" + holidayStarts
        + "&holidayEnds=" + holidayEnds
    );
    return callJobEndpoint(req);
  }

  public String getEvalStatsAlertFilter(Long functionId, String startTimeIso, String endTimeIso, String holidayStarts, String holidayEnds) throws Exception{
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + EVAL_FILTER + functionId
            + "?start=" + startTimeIso
            + "&end=" + endTimeIso
            + "&holidayStarts=" + holidayStarts
            + "&holidayEnds=" + holidayEnds
    );
    return callJobEndpoint(req);
  }

  public String evalAutoTune(long autotuneId, String startTimeIso, String endTimeIso, String holidayStarts, String holidayEnds) throws Exception{
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + EVAL_AUTOTUNE + autotuneId
        + "?start=" + startTimeIso
        + "&end=" + endTimeIso
        + "&holidayStarts=" + holidayStarts
        + "&holidayEnds=" + holidayEnds
    );
    return callJobEndpoint(req);
  }

  public String checkHasLabels(long functionId, String startTimeIso, String endTimeIso, String holidayStarts, String holidayEnds) throws IOException {
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + CHECK_HAS_LABELS + functionId
        + "?start=" + startTimeIso
        + "&end=" + endTimeIso
        + "&holidayStarts=" + holidayStarts
        + "&holidayEnds=" + holidayEnds
    );
    return callJobEndpoint(req);
  }

  public String initAutoTune(Long functionId, String startTimeIso, String endTimeIso, String autoTuneType, int nExpected, String holidayStarts, String holidayEnds)
      throws IOException {
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + INIT_AUTOTUNE + functionId
            + "?start=" + startTimeIso
            + "&end=" + endTimeIso
            + "&autoTuneType=" + autoTuneType
            + "&nExpected=" + nExpected
            + "&holidayStarts=" + holidayStarts
            + "&holidayEnds=" + holidayEnds
    );
    return callJobEndpoint(req);
  }

  public String runFunctionReplay(Long id, String startTimeISO, String endTimeISO, String tuningParameters,
      PerformanceEvaluationMethod evaluationMethod, double goal, boolean speedup)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(
        DETECTION_JOB_ENDPOINT + REPLAY_FUNCTION + id + "?start=" + startTimeISO + "&end=" + endTimeISO
            + "&tune=" + tuningParameters + "&goal=" + goal + "&evalMethod=" + evaluationMethod.name()
            + "&speedup=" + Boolean.toString(speedup));
    return callJobEndpoint(req);
  }
}
