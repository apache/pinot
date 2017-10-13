package com.linkedin.pinot.core.operator.transform;

/**
 * Convertor to convert and bucket a datetime value form an sdf format to an sdf format
 */
public class SDFToSDFConvertor extends DateTimeConvertor {

  public SDFToSDFConvertor(String inputFormat, String outputFormat, String outputGranularity) {
    super(inputFormat, outputFormat, outputGranularity);
  }

  @Override
  public Long convert(Object dateTimeValue) {
    Long dateTimeColumnValueMS = convertSDFToMillis(dateTimeValue);
    Long bucketedDateTimevalueMS = bucketDateTimeValueMS(dateTimeColumnValueMS);
    Long dateTimeValueConverted = convertMillisToSDF(bucketedDateTimevalueMS);
    return dateTimeValueConverted;
  }

}
