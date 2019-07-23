<#import "lib/utils.ftl" as utils>

<head>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans" rel="stylesheet">
</head>

<body style="background-color: #EDF0F3;">
<table border="0" cellpadding="0" cellspacing="0" style="width:100%; font-family: 'Proxima Nova','Arial','Helvetica Neue',Helvetica,sans-serif; font-size:14px; margin:0 auto; max-width: 50%; min-width: 700px; background-color: #F3F6F8;">
  <!-- White bar on top -->
  <tr>
    <td align="center" style="padding: 6px 24px;">
      <span style="color: red; font-size: 35px; vertical-align: middle;">&nbsp;&#9888;&nbsp;</span>
      <span style="color: rgba(0,0,0,0.75); font-size: 18px; font-weight: bold; letter-spacing: 2px; vertical-align: middle;">ACTION REQUIRED ON THIRDEYE ALERT</span>
    </td>
  </tr>

  <!-- Blue header on top -->
  <tr>
    <td style="font-size: 16px; padding: 12px; background-color: #0073B1; color: #FFF; text-align: center;">
      <span style="font-size: 18px; font-weight: bold; letter-spacing: 2px; vertical-align: middle;">Report: ${emailHeading}</span>
      <p>${emailDescription}</p>
    </td>
  </tr>

  <tr>
    <td>
      <table border="0" cellpadding="0" cellspacing="0" style="border:1px solid #E9E9E9; border-radius: 2px; width: 100%;">

        <!-- Only if whitelist entity exists -->
        <#if cid?has_content>
          <tr>
            <td style="padding: 12px;" align="center">
              <a href="${anomalyDetails[0].anomalyURL}${anomalyDetails[0].anomalyId}" target="_blank">
                <img style="width: 70%;" src="cid:${cid}" />
              </a>
            </td>
          </tr>
        </#if>

        <!-- List all the alerts -->
        <#list whitelistMetricToAnomaliesMap as metricName, whitelistAnomalies>
          <@utils.addBlock title="" align="left">
            <p>
              <span style="color: #1D1D1D; font-size: 20px; font-weight: bold; display:inline-block; vertical-align: middle;">Metric:&nbsp;</span>
              <span style="color: #606060; font-size: 20px; text-decoration: none; display:inline-block; vertical-align: middle; width: 70%; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${metricName}</span>
            </p>

            <!-- List all the anomalies under this detection -->
            <table border="0" width="100%" align="center" style="width:100%; padding:0; margin:0; border-collapse: collapse;text-align:left;">
              <#list whitelistAnomalies as anomaly>
                <#if anomaly.metric==metricName>
                  <tr style="text-align:center; background-color: #F6F8FA; border-top: 2px solid #C7D1D8; border-bottom: 2px solid #C7D1D8;">
                    <th style="text-align:left; padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Start / Duration</th>
                    <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Dimensions</th>
                    <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Current</th>
                    <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Predicted</th>
                  </tr>
                  <tr style="border-bottom: 1px solid #C7D1D8;">
                    <td style="padding: 6px 12px;white-space: nowrap;">
                      <div style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px;">${anomaly.startDateTime} ${anomaly.timezone}</div>
                      <span style="color: rgba(0,0,0,0.6); font-size:12px; line-height:16px;">${anomaly.duration}</span>
                      <a style="font-weight: bold; text-decoration: none; font-size:14px; line-height:20px; color: #0073B1;" href="${anomaly.anomalyURL}${anomaly.anomalyId}"
                         target="_blank">(view)</a>
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
          </@utils.addBlock>
        </#list>

        <#list entityToSortedAnomaliesMap as entityName, groupedAnomalies>
          <@utils.addBlock title="" align="left">
            <!-- Display Entity Name -->
            <p>
              <span style="color: #1D1D1D; font-size: 20px; font-weight: bold; display:inline-block; vertical-align: middle;">Entity:&nbsp;</span>
              <span style="color: #606060; font-size: 20px; text-decoration: none; display:inline-block; vertical-align: middle; width: 70%; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${entityName}</span>
            </p>

            <!-- List all the anomalies under this entity in a table -->
            <table border="0" width="100%" align="center" style="width:100%; padding:0; margin:0; border-collapse: collapse;text-align:left;">
              <tr style="text-align:center; background-color: #F6F8FA; border-top: 2px solid #C7D1D8; border-bottom: 2px solid #C7D1D8;">
                <th style="text-align:left; padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Feature</th>
                <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Criticality Score*</th>
                <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Current</th>
                <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Predicted</th>
              </tr>

              <#list groupedAnomalies as anomaly>
                <tr style="border-bottom: 1px solid #C7D1D8;">
                  <td style="padding: 6px 12px;white-space: nowrap;">
                    <a style="font-weight: bold; text-decoration: none; font-size:14px; line-height:20px; color: #0073B1;" href="${dashboardHost}/app/#/anomalies?anomalyIds=${anomalyToChildIdsMap[anomaly.groupKey + anomaly.startDateTime]}"
                       target="_blank">${anomaly.groupKey}</a>
                    <div style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px;">${anomaly.startDateTime} ${anomaly.timezone}</div>
                    <span style="color: rgba(0,0,0,0.6); font-size:12px; line-height:16px;">${anomaly.duration}</span>
                  </td>
                  <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">${anomaly.score}</td>
                  <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">${anomaly.currentVal}</td>
                  <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">
                    ${anomaly.baselineVal}
                    <div style="font-size: 12px; color:${anomaly.positiveLift?string('#3A8C18','#ee1620')};">(${anomaly.positiveLift?string('+','')}${anomaly.lift})</div>
                  </td>
                </tr>
              </#list>
            </table>

          </@utils.addBlock>
        </#list>

        <!-- Reference Links -->
        <#if referenceLinks?has_content>
          <@utils.addBlock title="Useful Links" align="left">
            <table border="0" align="center" style="table-layout: fixed; width:100%; padding:0; margin:0; border-collapse: collapse; text-align:left;">
              <#list referenceLinks?keys as referenceLinkKey>
                <tr style="border-bottom: 1px solid #C7D1D8; padding: 16px;">
                  <td style="padding: 6px 12px;">
                    <a href="${referenceLinks[referenceLinkKey]}" style="text-decoration: none; color:#0073B1; font-size:12px; font-weight:bold;">${referenceLinkKey}</a>
                  </td>
                </tr>
              </#list>
            </table>
          </@utils.addBlock>
        </#if>

      </table>
    </td>
  </tr>

  <tr>
    <td style="text-align: center; background-color: #EDF0F3; font-size: 12px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif; color: #737373; padding: 12px;">
      <p style="margin-top:0;"> You are receiving this email because you have subscribed to ThirdEye Alert Service for
        <strong>${alertConfigName}</strong>.</p>
      <p>
        If you have any questions regarding this report, please email
        <a style="color: #33aada;" href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
      </p>
    </td>
  </tr>

</table>
</body>
