<head>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans" rel="stylesheet">
</head>

<body style="background-color: #EDF0F3;">
  <table border="0" cellpadding="0" cellspacing="0" width="100%" style="width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:16px;line-height:normal;margin:0 auto; max-width: 700px; background-color: #F3F6F8; margin: 0 auto;">
    <tr style="background-color: #F3F6F8;">
      <td align="left" style="padding: 12px 24px; height: 60px; background-color: #F6F8FA;" colspan="2">
        <img width="35" height="35" alt="logo" src="https://static.licdn-ei.com/scds/common/u/images/email/logos/logo_shift_inbug_82x82_v1.png"
          style="vertical-align: middle; display: inline-block; margin-right: 8px; background: white;">
        <span style="color: rgba(0,0,0,0.75);font-size: 18px; font-weight: bold; letter-spacing: 2px; display: inline-block;vertical-align: middle;">THIRDEYE</span>
      </td>
    </tr>

    <tr>
      <td>
        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background-color:white; border:1px solid #E9E9E9; border-radius: 2px; width: 100%;">
          <tr>
            <td style="padding: 32px; background-color: #0073B1; color: #FFF; text-align: center" colspan="2">
              <p style="font-size: 20px; font-weight: 500; margin-bottom: 12px;">${anomalyCount} anomalies were detected </p>
              <p style="font-size: 16px; font-weight: 300; line-height:20px; color:#FFF">between ${startTime} ${timeZone} and ${endTime} ${timeZone}.</p>
              <p style="margin-top: 0px; font-size:14px;  margin-bottom: 25px;">Below are the list of all anomalies from alerts subscribed by
                <strong>${alertConfigName}</strong>.</p>
              <#if isGroupedAnomaly>
                <a style="margin-top: 0px; padding: 4px 12px; border-radius: 2px; border: 1px solid #FFF; font-size: 16px; font-weight: bold; color: white; text-decoration: none; line-height: 32px;"
                  href="${dashboardHost}/thirdeye#anomalies?anomaliesSearchMode=groupId&anomalyGroupIds=${groupId}">View all in ThirdEye</a>
                <#else>
                  <a style="margin-top: 0px; padding: 4px 12px; border-radius: 2px; border: 1px solid #FFF; font-size: 16px; font-weight: bold; color: white; text-decoration: none; line-height: 32px;"
                    href="${dashboardHost}/thirdeye#anomalies?anomaliesSearchMode=id&anomalyIds=${anomalyIds}">View all in ThirdEye</a>
              </#if>
              </p>
            </td>
          </tr>
          <tr>
            <td colspan="2" style="border-bottom: 1px solid #E9E9E9;">
            </td>
          </tr>

          <#if cid?has_content>
            <tr>
              <td style="padding: 24px;" colspan="2" align="center">
                <a href="${anomalyDetails[0].anomalyURL}${anomalyDetails[0].anomalyId}" target="_blank">
                  <img style="width: 70%;" src="cid:${cid}" \>
                </a>
              </td>
            </tr>
          </#if>

          <#list metricAnomalyDetails?keys as metric>
            <tr>
              <td style="border-bottom: 1px solid rgba(0,0,0,0.15); padding: 24px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;" colspan="2" align="left">
                <p style="margin-left: 24px;">
                  <span style="color: #606060; font-size: 20px; line-height: 24px; margin-right: 16px;">Metric</span>
                  <span style="color: #1D1D1D; font-size: 20px; font-weight: bold; line-height: 24px;">${metric}</span>
                </p>
                <#list functionAnomalyDetails?keys as function>
                  <#assign newTable = false>
                  <#list functionAnomalyDetails[function] as anomaly>
                    <#if anomaly.metric==metric>
                      <#assign newTable=true>
                    </#if>
                  </#list>
                  <#if newTable>
                  <p style="margin: 24px;">
                      <span style="color: #606060; font-size: 14px; line-height: 20px; margin-right: 16px;">Alert</span>
                      <a href="${dashboardHost}/app/#/manage/alerts/${functionToId[function]?string.computer}" target="blank" style="text-decoration: none; color: #0B5EA1; font-size: 14px; font-weight: bold; line-height: 20px;">${function}</a>
                  </p>    
                  </#if>           
                  <table border="0" width="100%" align="center" style="width:100%; padding:0; margin:0; border-collapse: collapse;text-align:left;">
                    <#list functionAnomalyDetails[function] as anomaly>
                      <#if anomaly.metric==metric>
                        <#if newTable>
                          <tr style="background-color: #F6F8FA; border-top: 2px solid #C7D1D8; border-bottom: 2px solid #C7D1D8;">
                            <th style="padding: 4px 24px; font-size: 12px; font-weight: bold; line-height: 20px;">Start / Duration</th>
                            <th style="padding: 4px 24px; font-size: 12px; font-weight: bold; line-height: 20px;">Dimensions</th>
                            <th style="padding: 4px 24px; font-size: 12px; font-weight: bold; line-height: 20px;">Current</th>
                            <th style="padding: 4px 24px; font-size: 12px; font-weight: bold; line-height: 20px;">Predicted</th>
                            <th style="padding: 4px 24px; font-size: 12px; font-weight: bold; line-height: 20px; display: none;">Wow</th>
                            <th style="padding: 4px 24px; font-size: 12px; font-weight: bold; line-height: 20px; display: none;">Wo2W</th>
                          </tr>
                        </#if>
                        <#assign newTable = false>
                        <tr style="border-bottom: 1px solid #C7D1D8;">
                          <td style="padding: 14px 24px;white-space: nowrap;">
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
              </td>
            </tr>
          </#list>

          <#if holidays?has_content>
            <tr>
              <td style="padding: 24px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;" colspan="2" align="center">
                <p style="font-size:20px; line-height:24px; color:#1D1D1D; font-weight: 500; margin:0; padding:0;">Holidays</p>
                <#list holidays as holiday>
                  <div style="padding-top: 16px;">
                    <a href="https://www.google.com/search?q=${holiday.name}" style="text-decoration: none; color:#0073B1; font-size:14px; font-weight:bold; line-height:20px; margin-bottom: 0;">${holiday.name}</a> 
                    <span style="font-size: 14px; color:#606060; line-height:20px;">(${holiday.startTime?number_to_date})</span>
                  </div>
                  <#if holiday.targetDimensionMap?has_content>
                    <#list holiday.targetDimensionMap?keys as dimension>
                        <span style="color: rgba(0,0,0,0.6); line-height:16px; font-size: 12px;">${holiday.targetDimensionMap[dimension]?join(",")}</span>
                    </#list>
                  </#if>
                </#list>
              </td>
            </tr>
          </#if>


          <tr>
            <td colspan="2" style="border-bottom: 1px solid #E9E9E9;">
            </td>
          </tr>

          <tr>
            <td style="text-align: center; background-color: #EDF0F3; font-size: 12px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif; color: #737373; padding: 24px; font-size:14px;"
              colspan="2">
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