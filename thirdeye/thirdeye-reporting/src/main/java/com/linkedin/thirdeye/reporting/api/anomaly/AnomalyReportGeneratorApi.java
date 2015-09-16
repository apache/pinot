package com.linkedin.thirdeye.reporting.api.anomaly;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.reporting.api.DBSpec;
import com.linkedin.thirdeye.reporting.api.ReportConfig;
import com.linkedin.thirdeye.reporting.api.TableSpec;

public class AnomalyReportGeneratorApi {

  public AnomalyReportGeneratorApi() {

  }

  public Map<String, AnomalyReportTable> getAnomalies(ReportConfig reportConfig, TableSpec tableSpec, String collection) throws IOException {

    DBSpec dbSpec = reportConfig.getDbconfig();
    Map<String, AnomalyReportTable> anomalyTables = new HashMap<String, AnomalyReportTable>();

    for (String metric : tableSpec.getMetrics()) {

      AnomalyDatabaseConfig dbConfig = new AnomalyDatabaseConfig(dbSpec.getUrl(), dbSpec.getFunctionTableName(), dbSpec.getAnomalyTableName(),
          dbSpec.getUser(), dbSpec.getPassword(), dbSpec.isUseConnectionPool());

      AnomalyReportGenerator anomalyReportGenerator = new AnomalyReportGenerator(dbConfig);
      anomalyTables.put(metric, anomalyReportGenerator.getAnomalyTable(collection, metric,
          reportConfig.getStartTime().getMillis(), reportConfig.getEndTime().getMillis(), 20, reportConfig.getTimezone()));
    }
    return anomalyTables;
  }



}
