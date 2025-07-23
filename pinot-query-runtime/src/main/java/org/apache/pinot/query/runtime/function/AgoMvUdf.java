package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


@AutoService(Udf.class)
public class AgoMvUdf extends Udf.FromAnnotatedMethod {

  public AgoMvUdf()
      throws NoSuchMethodException {
    super(DateTimeFunctions.class.getMethod("agoMV", String[].class));
  }

  @Override
  public String getMainName() {
    return "agomv";
  }

  @Override
  public String getDescription() {
    return "Returns an array of timestamps with the result of calling [ago] on each element of the input array. "
        + "The input array should contain ISO-8601 duration strings, and the output array will contain the "
        + "corresponding epoch millis for each duration.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    /// TODO: The UdfTest framework doesn't support UDFs whose result depends on current time
    return Map.of();
  }
}
