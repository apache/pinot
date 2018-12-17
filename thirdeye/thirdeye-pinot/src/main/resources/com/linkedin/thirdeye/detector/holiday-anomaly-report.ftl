<#import "lib/utils.ftl" as utils>

<head>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans" rel="stylesheet">
</head>

<body style="background-color: #EDF0F3;">
<table border="0" cellpadding="0" cellspacing="0" style="width:100%; font-family: 'Proxima Nova','Arial','Helvetica Neue',Helvetica,sans-serif; font-size:14px; margin:0 auto; max-width: 50%; background-color: #F3F6F8;">
  <tr>
    <td align="left" style="padding: 12px 24px;">
      <img width="35" height="35" alt="logo" src="https://static.licdn-ei.com/scds/common/u/images/email/logos/logo_shift_inbug_82x82_v1.png"
           style="vertical-align: middle; display: inline-block; background: white;">
      <span style="color: rgba(0,0,0,0.75);font-size: 18px; font-weight: bold; letter-spacing: 2px; display: inline-block;vertical-align: middle;">THIRDEYE</span>
    </td>
  </tr>

  <tr>
    <td>
      <table border="0" cellpadding="0" cellspacing="0" style="border:1px solid #E9E9E9; border-radius: 2px; width: 100%;">
        <tr>
          <td style="padding: 12px; background-color: #0073B1; color: #FFF; text-align: center;">
            <h2>ACTION REQUIRED</h2>
            <p>
              <#if anomalyCount == 1>
                <strong>An anomaly</strong> was detected between <strong>${startTime} and ${endTime} (${timeZone})</strong>
              <#else>
                <strong>${anomalyCount} anomalies</strong> were detected between <strong>${startTime} and ${endTime} (${timeZone})</strong>
              </#if>
              <#if metricsMap?size == 1>
                <#list metricsMap?keys as id>
                  on metric <div style="padding: 10px;"><strong><a style="color: white;" href="${dashboardHost}/app/#/rootcause?metricId=${id}">${metricsMap[id].name}</a></strong></div>
                </#list>
              <#else>
                on metrics
                <#list metricsMap?keys as id>
                  <div style="padding: 10px;">
                    <strong><a style="padding: 10px; color: white;" href="${dashboardHost}/app/#/rootcause?metricId=${id}">${metricsMap[id].name}</a></strong>
                  </div>
                </#list>
              </#if>
            </p>
            <p>
              <#if isGroupedAnomaly>
                <a style="padding: 6px 12px; border-radius: 2px; border: 1px solid #FFF; font-size: 16px; font-weight: bold; color: white; text-decoration: none; line-height: 32px;" href="${dashboardHost}/thirdeye#anomalies?anomaliesSearchMode=groupId&anomalyGroupIds=${groupId}">Investigate all in ThirdEye</a>
              <#else>
                <a style="padding: 6px 12px; border-radius: 2px; border: 1px solid #FFF; font-size: 16px; font-weight: bold; color: white; text-decoration: none; line-height: 32px;" href="${dashboardHost}/thirdeye#anomalies?anomaliesSearchMode=id&anomalyIds=${anomalyIds}">Investigate all in ThirdEye</a>
              </#if>
            </p>
          </td>
        </tr>

          <#if cid?has_content>
            <tr>
              <td style="padding: 12px;" align="center">
                <a href="${anomalyDetails[0].anomalyURL}${anomalyDetails[0].anomalyId}" target="_blank">
                  <img style="width: 70%;" src="cid:${cid}" \>
                </a>
              </td>
            </tr>
          </#if>

          <#list metricAnomalyDetails?keys as metric>
            <@utils.addBlock title="" align="left">
                <p>
                  <span style="color: #606060; font-size: 20px;">Metric</span>
                  <span style="color: #1D1D1D; font-size: 20px; font-weight: bold;">${metric}</span>
                </p>
                <#list functionAnomalyDetails?keys as function>
                  <#assign newTable = false>
                  <#list functionAnomalyDetails[function] as anomaly>
                    <#if anomaly.metric==metric>
                      <#assign newTable=true>
                    </#if>
                  </#list>
                  <#if newTable>
                  <p>
                    <span style="color: #606060;">Alert</span>
                    <a href="${dashboardHost}/app/#/manage/alert/${functionToId[function]?string.computer}" target="blank" style="text-decoration: none; color: #0B5EA1; font-weight: bold;">${function}</a>
                  </p>
                  </#if>
                  <table border="0" width="100%" align="center" style="width:100%; padding:0; margin:0; border-collapse: collapse;text-align:left;">
                    <#list functionAnomalyDetails[function] as anomaly>
                      <#if anomaly.metric==metric>
                        <#if newTable>
                          <tr style="text-align:center; background-color: #F6F8FA; border-top: 2px solid #C7D1D8; border-bottom: 2px solid #C7D1D8;">
                            <th style="text-align:left; padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Start / Duration</th>
                            <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Dimensions</th>
                            <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Current</th>
                            <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Predicted</th>
                            <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px; display: none;">Wow</th>
                            <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px; display: none;">Wo2W</th>
                          </tr>
                        </#if>
                        <#assign newTable = false>
                        <tr style="border-bottom: 1px solid #C7D1D8;">
                          <td style="padding: 6px 12px;white-space: nowrap;">
                            <a style="font-weight: bold; text-decoration: none; font-size:14px; line-height:20px; color: #0073B1;" href="${anomaly.anomalyURL}${anomaly.anomalyId}"
                               target="_blank">${anomaly.startDateTime} ${anomaly.timezone}</a>
                            <div style="color: rgba(0,0,0,0.6); font-size:12px; line-height:16px;">${anomaly.duration}</div>
                          </td>
                          <td style="word-break: break-all; width: 200px; padding-right:4px 20px 4px 0">
                            <#list anomaly.dimensions as dimension>
                              <span style="color: rgba(0,0,0,0.6); font-size: 12px; line-height: 16px;">${dimension}</span>
                              </br>
                            </#list>
                          </td>
                          <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">${anomaly.currentVal}</td>
                          <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">
                            ${anomaly.baselineVal}
                            <div style="font-size: 12px; color:${anomaly.positiveLift?string('#3A8C18','#ee1620')};">(${anomaly.positiveLift?string('+','')}${anomaly.lift})</div>
                          </td>
                        </tr>
                      </#if>
                    </#list>
                  </table>
                </#list>
            </@utils.addBlock>
          </#list>

          <#if referenceLinks?has_content>
            <@utils.addBlock title="Useful Links" align="left">
              <#list referenceLinks?keys as referenceLinkKey>
                <div style="padding: 10px;">
                  <a href="${referenceLinks[referenceLinkKey]}" style="font-weight: bold; text-decoration: none; color:#0073B1;">${referenceLinkKey}</a>
                </div>
              </#list>
            </@utils.addBlock>
          </#if>

          <#if holidays?has_content>
            <@utils.addBlock title="Holidays" align="left">
              <table border="0" align="center" style="table-layout: fixed; width:100%; padding:0; margin:0; border-collapse: collapse; text-align:left;">
                <#list holidays as holiday>
                  <tr style="border-bottom: 1px solid #C7D1D8; padding: 16px;">

                    <td style="padding: 6px 12px;">
                      <a href="https://www.google.com/search?q=${holiday.name}" style="text-decoration: none; color:#0073B1; font-size:12px; font-weight:bold;">${holiday.name}</a>
                    </td>

                    <td style="padding: 6px 12px;">
                      <span style="font-size: 12px; color:#606060;">(${holiday.startTime?number_to_date})</span>
                    </td>

                    <#if holiday.targetDimensionMap["countryCode"]??>
                      <td style="padding: 6px 12px;">
                        <span style="color: rgba(0,0,0,0.6); font-size: 12px;">${holiday.targetDimensionMap["countryCode"]?join(", ")}</span>
                      </td>
                    </#if>

                  </tr>
                </#list>
              </table>
            </@utils.addBlock>
          </#if>

        <tr>
          <td style="text-align: center; background-color: #EDF0F3; font-size: 12px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif; color: #737373; padding: 12px;">
            <p style="margin-top:0;"> You are receiving this email because you have subscribed to ThirdEye Alert Service for
              <strong>${alertConfigName}</strong>.</p>
            <p>
              If you have any questions regarding this report, please email
              <a style="color: #33aada;" href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
            </p>
            <p style="margin-bottom:0; margin-top: 24px;">
              &#169; 2017, LinkedIn Corporation, 1000 W Maude Ave, CA 94085, USA
            </p>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</body>