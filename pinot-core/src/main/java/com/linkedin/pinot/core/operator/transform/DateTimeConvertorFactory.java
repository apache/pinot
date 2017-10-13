package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;
import com.linkedin.pinot.common.utils.time.DateTimeFieldSpecUtils;

public class DateTimeConvertorFactory {

  /**
   * Method to get dateTimeConvertor depending on the input and output format
   * @param inputFormat
   * @param outputFormat
   * @return
   */
  public static DateTimeConvertor getDateTimeConvertorFromFormats(String inputFormat, String outputFormat, String outputGranularity) {
    DateTimeConvertor dateTimeConvertor = null;
    TimeFormat inputTimeFormat = DateTimeFieldSpecUtils.getTimeFormatFromFormat(inputFormat);
    TimeFormat outputTimeFormat = DateTimeFieldSpecUtils.getTimeFormatFromFormat(outputFormat);
    if (inputTimeFormat.equals(TimeFormat.EPOCH)) {
      if (outputTimeFormat.equals(TimeFormat.EPOCH)) {
        dateTimeConvertor = new EpochToEpochConvertor(inputFormat, outputFormat, outputGranularity);
      } else {
        dateTimeConvertor = new EpochToSDFConvertor(inputFormat, outputFormat, outputGranularity);
      }
    } else {
      if (outputTimeFormat.equals(TimeFormat.EPOCH)) {
        dateTimeConvertor = new SDFToEpochConvertor(inputFormat, outputFormat, outputGranularity);
      } else {
        dateTimeConvertor = new SDFToSDFConvertor(inputFormat, outputFormat, outputGranularity);
      }
    }
    return dateTimeConvertor;
  }
}
