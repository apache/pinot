import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;

public class Test {

  public static void main(String[] args) {
    BufferedReader br = null;

    try {

      String sCurrentLine;

      br = new BufferedReader(new FileReader("/Users/npawar/Documents/mysql_imports/anomaly_functions.csv"));

      while ((sCurrentLine = br.readLine()) != null) {
        System.out.println(sCurrentLine);
        String[] tokens = sCurrentLine.split("\"");
        List<String> cleanTokens = new ArrayList<String>();
        for (int i = 0; i < tokens.length; i++) {
          String token = tokens[i];
          if (StringUtils.isBlank(token)) {
            cleanTokens.add(token);
            continue;
          }

          if ((i==0) || (token.equals(";"))) continue;
          if ((token.charAt(0) == ';')) {
            token = token.replaceAll(";", "");
          }
          cleanTokens.add(token);
        }
        System.out.println(cleanTokens);
        AnomalyFunctionSpec spec = new AnomalyFunctionSpec();
        spec.setCollection(cleanTokens.get(0));
        spec.setMetric(cleanTokens.get(1));
        spec.setType(cleanTokens.get(2));
        spec.setIsActive(cleanTokens.get(3).equals("1"));
        spec.setCron(cleanTokens.get(4));
        spec.setBucketSize(Integer.parseInt(cleanTokens.get(5)));
        spec.setBucketUnit(getTimeUnit(cleanTokens.get(6)));
        spec.setWindowSize(Integer.parseInt(cleanTokens.get(7)));
        spec.setWindowUnit(getTimeUnit(cleanTokens.get(8)));
        spec.setWindowDelay(Integer.parseInt(cleanTokens.get(9)));
        spec.setProperties(cleanTokens.get(10));
        spec.setExploreDimensions(cleanTokens.get(11));
        spec.setFilters(cleanTokens.get(12));
        spec.setWindowDelayUnit(getTimeUnit(cleanTokens.get(13)));
        spec.setFunctionName(cleanTokens.get(14));
        spec.setMetricFunction(MetricAggFunction.SUM);

        System.out.println(spec);
      }


    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (br != null)br.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }

  }

  public static TimeUnit getTimeUnit(String unit) {
    switch(unit) {
      case "4" :
        return TimeUnit.MINUTES;
      case "5":
        return TimeUnit.HOURS;
      case "6":
        return TimeUnit.DAYS;
    }
    return null;
  }
}
