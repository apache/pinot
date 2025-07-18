package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.core.operator.transform.function.DateTimeTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class ToDateTimeUdf extends Udf.FromAnnotatedMethod {

  public ToDateTimeUdf() throws NoSuchMethodException {
    super(DateTimeFunctions.class, "toDateTime", long.class, String.class);
  }

  @Override
  public String getDescription() {
    return "Converts epoch millis to a DateTime string represented by the given pattern. Optionally, a timezone can be provided.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(
        UdfSignature.of(
            UdfParameter.of("mills", FieldSpec.DataType.LONG)
                .withDescription("A long value representing epoch millis, "
                    + "e.g., 1577836800000L for 2020-01-01T00:00:00Z"),
            UdfParameter.of("format", FieldSpec.DataType.STRING)
                .withDescription("A string literal representing the date format, "
                    + "e.g., 'yyyy-MM-dd'T'HH:mm:ss'Z' or 'yyyy-MM-dd'")
                .asLiteralOnly(),
            UdfParameter.result(FieldSpec.DataType.STRING) // Return type is single value STRING
        ))
        .addExample("UTC ISO8601", 1577836800000L, "yyyy-MM-dd'T'HH:mm:ss'Z'", "2020-01-01T00:00:00Z")
        .addExample("Date only", 1577836800000L, "yyyy-MM-dd", "2020-01-01")
        //.addExample(UdfExample.create("null millis", null, "yyyy-MM-dd", null).withoutNull("1970-01-01"))
        //.addExample("null format", 1577836800000L, null, null)
        .build()
        .generateExamples();
  }

  @Override
  public Map<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunctions() {
    // No transform function for toDateTime, similar to ArrayMaxUdf
    return Map.of();
  }
}

