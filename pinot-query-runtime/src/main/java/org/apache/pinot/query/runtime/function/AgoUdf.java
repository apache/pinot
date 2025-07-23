package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


@AutoService(Udf.class)
public class AgoUdf extends Udf.FromAnnotatedMethod {

  public AgoUdf()
      throws NoSuchMethodException {
    super(DateTimeFunctions.class.getMethod("ago", String.class));
  }

  @Override
  public String getMainName() {
    return "ago";
  }

  @Override
  public String getDescription() {
    return "Return time as epoch millis before the given period (in ISO-8601 duration format). For example, "
        + "`ago('PT1H')` returns the epoch millis of one hour ago. The period can be negative, in which case it "
        + "returns the epoch millis in the future. For example, `ago('P2DT3H4M')` returns the epoch millis of "
        + "two days, three hours, and four minutes in the future.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    /// TODO: The UdfTest framework doesn't support UDFs whose result depends on current time
    return Map.of();
  }
}
