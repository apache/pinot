<table border="0" cellpadding="0" cellspacing="0"
           style="padding:0px; width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:15px;line-height:normal;margin:0 auto; padding:0px 0px 10px 0px; background-color: #fff; margin: 0 auto;">
  <tr style="height:50px; background-color: #F3F6F8;">
    <td align="left" style="padding: 10px 24px;height:50px;" colspan="2">
      <img width="35" height="35" alt="logo" src="https://static.licdn-ei.com/scds/common/u/images/email/logos/logo_shift_inbug_82x82_v1.png" style="vertical-align: middle; display: inline-block; padding-right: 8px">
      <span style="color: #737373;font-size: 15px;display: inline-block;vertical-align: middle;">THIRDEYE</span>
    </td>
  </tr>

  <tr>
    <td style="padding: 0 24px;" colspan="2">
      <p style="font-size: 20px; font-weight: 600;">Hi,</p>
      <p style="color: #737373; font-size: 14px;">You are receiving this email because you have subscribed to ThirdEye Alert Service for <strong>'${alertConfigName}'</strong>.</p>
      <p style="color: #737373; font-size: 14px;">
        Analysis Start: ${dateFormat(startTime)}<br/>
        Analysis End: ${dateFormat(endTime)}
      </p>
      <p style="color: #737373; font-size: 14px;"> ThirdEye has analyzed your dataset and has detected
        <b>${anomalyCount} ${(anomalyCount == 1)?string("anomaly", "anomalies")}.</b> Below is the full list of anomalies detected during this time period.</p>
    </td>
  </tr>

  <#if includeSummary?has_content>
  <tr>
    <td style="padding: 0 24px;" colspan="2">
      <div style="padding: 24px; background-color:#edf0f3;">
        <table border="0" align="center"  width="100%" style="width:100%;border-collapse: collapse; border-spacing: 0; margin-bottom: 15px;border-color:#ddd; padding:24px;">
          <tr>
            <td colspan="100%" style="width:100%; padding-bottom:8px;">
            <span style="color: #737373">Summary</span></td>
          </tr>
          <tr>
            <td colspan="33%" style="width: 33%; padding-bottom:8px;">
              <div style="color: #737373">Anomalies:</div>
              <div style="color: #737373">${anomalyCount}</div>
            </td>
            <td colspan="33%" style="width: 33%; padding-bottom:8px;">
              <div style="color: #737373">Metrics:</div>
              <div style="color: #737373">${metricsCount}</div>
            </td>
            <td colspan="33%" style="width: 33%; padding-bottom:8px;">
              <div style="color: #737373">Notified:</div>
              <div style="color: #737373">${notifiedCount}</div>
            </td>
          </tr>
          <tr>
            <td colspan="33%" style="width: 33%; padding-bottom:8px;">
              <div style="color: #737373">Feedback:</div>
              <div style="color: #737373">${feedbackCount}</div>
            </td>
            <td colspan="33%" style="width: 33%; padding-bottom:8px;">
              <div style="color: #737373">True Alert:</div>
              <div style="color: #737373">${trueAlertCount}</div>
            </td>
            <td colspan="33%" style="width: 33%; padding-bottom:8px;">
              <div style="color: #737373">False Alert:</div>
              <div style="color: #737373">${falseAlertCount}</div>
            </td>
          </tr>
          <tr>
            <td colspan="100%" style="width: 100%; padding-bottom:8px;">
              <div style="color: #737373">Non Actionable</div>
              <div style="color: #737373">${nonActionableCount}</div>
            </td>
          </tr>
        </table>
      </div>
    </td>
  </tr>
  </#if>

  <#if anomalyDetails?has_content>
    <tr>
      <td style="padding: 24px;" colspan="2">
        <table align="center" border="0" width="100%" style="width:100%; border-collapse:collapse; border-spacing:0;margin-bottom:24px;font-size: 12px;">
          <tr>
            <th style="border:1px solid #CCC; padding:0 8px;">Metric</th>
            <th style="border:1px solid #CCC; padding:0 8px;">Dimension</th>
            <th style="border:1px solid #CCC; padding:0 8px; width:18%;" colspan="18%">Duration</th>
            <th style="border:1px solid #CCC; padding:0 8px;">Details</th>
            <#if includeSummary?has_content>
              <th style="border:1px solid #CCC; padding:0 8px;">Status</th>
            </#if>
            <th style="border:1px solid #CCC; padding:0 8px;">Investigate</th>
            <t
          </tr>

          <#list anomalyDetails as r>
            <tr>
              <td style="border:1px solid #CCC; padding:0 8px;">${r.metric}</td>
              <td style="border:1px solid #CCC; padding:0 8px;">${r.dimensions}</td>
              <td style="border:1px solid #CCC; padding:0 8px; width:18%;" colspan="18%">${r.duration}</td>
              <td style="border:1px solid #CCC; padding:0 8px;">
                <b>Change: </b>${r.lift} <br>
                <b>Current: </b>${r.currentVal} <br>
                <b>Baseline: </b>${r.baselineVal}
              </td>
              <#if includeSummary?has_content>
                <td style="border:1px solid #CCC; padding:0 8px;">${r.feedback}</td>
              </#if>
              <td style="border:1px solid #CCC; padding:0 8px;"><a href="${r.anomalyURL}${r.anomalyId}" target="_blank" style="color: white;font-weight: 600;background-color: #0084bf;font-size: 12px;padding: 0 8px;line-height: 20px;border-radius: 2px;cursor: pointer;display: inline-block;border: 1px solid transparent;text-decoration: none;">Investigate</a></td>
            </tr>
          </#list>

        </table>
      </td>
    </tr>
  </#if>
  <!-- OLD  -->
<#if (groupedAnomalyResults?has_content)>
  <tr>
    <td>
      <table align="left" border="1" cellpadding="4px"
             style="width:100%;border-collapse: collapse; border-spacing: 0 margin-bottom: 15px; border: 1px solid #ddd;">
        <tr>
          <th style="padding:5px 3px">Dimensions</th>
          <th>No.</th>
          <th>Time (${timeZone})</th>
          <th>Reason</th>
          <th>Function</th>
        </tr>
        <#assign anomalySequenceIndex = 1>
        <#list groupedAnomalyResults?keys as dimensionMap>
          <#assign results = groupedAnomalyResults[dimensionMap]>
          <#assign dimensionStr = dimensionMap>
          <#list results as r>
            <tr>
              <#if r == results?first>
                <td rowspan="${results?size}" style="padding:5px 3px">${dimensionStr}</td>
              </#if>
              <td style="padding:5px 3px">
              ${anomalySequenceIndex?c}
              </td>
              <td style="padding:5px 3px">

                <#if r.endTime??>
                ${dateFormat(r.startTime)} to ${dateFormat(r.endTime)}
                <#else>
                ${dateFormat(r.startTime)}
                </#if>
              </td>
              <td style="padding:5px 3px">${r.message!"N/A"}
              </td>
              <td style="padding:5px 3px">
              ${r.function.functionName} (${r.function.type})
              </td>
            </tr>
            <#assign anomalySequenceIndex = anomalySequenceIndex + 1>
          </#list>
        </#list>
      </table>
    </td>
  </tr>
</#if>
  <tr>
    <td>
      Go to <a
        href="${dashboardHost}/dashboard#view=anomalies&dataset=${collection}&compareMode=WoW&aggTimeGranularity=${windowUnit}&metrics=${metric}<#if (anomalyCount > 0) >&currentStart=${startTime?c}&currentEnd=${endTime?c}</#if>"
        target="_top">ThirdEye Anomalies Dashboard</a>
    </td>
  </tr>
<#if (metricDimensionValueReports?has_content)>
  <#setting time_zone=timeZone >
  <tr>
    <td>
      <hr/>
      <p>
        Report start time : ${dateFormat(reportStartDateTime)}
      </p>
    </td>
  </tr>
  <#assign reportCount = 1>
  <#list metricDimensionValueReports as metricReport>
    <tr>
      <td><b>
      ${reportCount} - <a href="${dashboardHost}/dashboard#view=compare&dataset=${collection}&metrics=${metricReport.metricName}&dimensions=${metricReport.dimensionName}&compareMode=WoW&aggTimeGranularity=HOURS&currentStart=${metricReport.currentStartTime?c}&currentEnd=${metricReport.currentEndTime?c}&baselineStart=${metricReport.baselineStartTime?c}&baselineEnd=${metricReport.baselineEndTime?c}">
      ${metric} by ${metricReport.dimensionName}
      </a>
      </b></td>
    </tr>
    <#assign subDimensionValueMap = metricReport.subDimensionValueMap >
    <tr>
      <td>
        <table align="left" border="1"
               style="width:100%;border-collapse: collapse; border-spacing: 0 margin-bottom:15px;border-color:#ddd;"
               cellspacing="0px" cellpadding="4px">
          <tr>
            <td>${metricReport.dimensionName}</td>
            <td>Share</td>
            <td>Total</td>
            <#assign itrCount = 1 >
            <#list subDimensionValueMap?keys as groupByDimension>
              <#assign timeBucketValueMap = subDimensionValueMap[groupByDimension]>
              <#if itrCount == 1>
                <#list timeBucketValueMap?keys as timeBucket>
                  <td>
                  ${timeBucket?number?number_to_time?string("HH:mm")}
                  </td>
                </#list>
              </#if>
              <#assign itrCount = itrCount + 1>
            </#list>
          </tr>
          <#list subDimensionValueMap?keys as dimensionKey>
            <tr>
              <td>
              ${dimensionKey}
              </td>
              <td>${metricReport.subDimensionShareValueMap[dimensionKey]}</td>
              <td>${metricReport.subDimensionTotalValueMap[dimensionKey]}</td>
              <#assign timevalmap = subDimensionValueMap[dimensionKey] >
              <#list timevalmap?keys as timebucketkey>
                <td> ${timevalmap[timebucketkey]}%</td>
              </#list>
            </tr>
          </#list>
        </table>
      </td>
    </tr>

    <#assign reportCount = reportCount + 1>
  </#list>
</#if>
<tr>
  <td style="font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:14px; color: #737373;font-weight:300; text-align: center;" colspan="2">
    <p>If you have any questions regarding this report, please email <br/>
      <a href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
    </p>
    <p>
      Thanks,<br>
      ThirdEye Team
    </p>
  </td>
</tr>
</table>
