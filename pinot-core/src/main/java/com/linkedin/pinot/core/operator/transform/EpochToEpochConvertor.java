package com.linkedin.pinot.core.operator.transform;

/**
 * Convertor to convert and bucket a datetime value form an epoch format to an epoch format
 */
public class EpochToEpochConvertor extends DateTimeConvertor {

  public EpochToEpochConvertor(String inputFormat, String outputFormat, String outputGranularity) {
    super(inputFormat, outputFormat, outputGranularity);
  }

  @Override
  public Long convert(Object dateTimeValue) {
    Long dateTimeColumnValueMS = convertEpochToMillis(dateTimeValue);
    Long bucketedDateTimevalueMS = bucketDateTimeValueMS(dateTimeColumnValueMS);
    Long dateTimeValueConverted = convertMillisToEpoch(bucketedDateTimevalueMS);
    return dateTimeValueConverted;
  }

}
