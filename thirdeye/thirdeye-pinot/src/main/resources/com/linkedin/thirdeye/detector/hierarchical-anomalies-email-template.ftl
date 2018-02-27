<head>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans" rel="stylesheet">
</head>
<body>
<table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:16px;line-height:normal;margin:0 auto; padding: 24px; background-color: #F3F6F8; margin: 0 auto;">
  <tr style="background-color: #F3F6F8;">
    <td align="left" style="padding: 12px; padding-top:0; height:50px;" colspan="2">
      <img width="35" height="35" alt="logo" src="https://static.licdn-ei.com/scds/common/u/images/email/logos/logo_shift_inbug_82x82_v1.png" style="vertical-align: middle; display: inline-block; padding-right: 8px">
      <span style="color: rgba(0,0,0,0.55);font-size: 16px;display: inline-block;vertical-align: middle;">THIRDEYE - Anomaly Detection</span>
    </td>
  </tr>

  <tr>
    <td>
      <table border="0" cellpadding="0" cellspacing="0" width="100%"
           style="background-color:white; border:1px solid #E9E9E9; border-radius: 2px; width: 100%;">
        <tr>
          <td style="padding: 0 24px;" colspan="2">
              <p style="font-size: 20px; margin-bottom: 8px;">Hi,</p> <br>
                <p style="color: rgba(0,0,0,0.55); margin-top: 0px;"> ThirdEye has detected <strong style="color: black;">${anomalyCount} ${(anomalyCount == 1)?string("anomaly", "anomalies")}</strong>. Below is a summary, please go <strong>
                    <a style="color:#33aada;" href="${dashboardHost}/thirdeye#anomalies?anomaliesSearchMode=id&anomalyIds=${anomalyIds}">here</a>
                  </strong> for a detailed view.
                </p>
            </p>
          </td>
        </tr>
        <tr>
          <td colspan="2" style="border-bottom: 1px solid #E9E9E9;">
          </td>
        </tr>

        <tr>
          <td style="padding: 0 24px;" colspan="2">
            <p style="color: rgba(0,0,0,0.55); font-size:16px; margin-bottom:0;">DATASET</p>
            <p style="margin-top:8px; margin-bottom:24px; font-size:20px">${datasets}</p>
          </td>
        </tr>

        <tr>
          <td style="padding: 0 24px;" colspan="2">
            <table align="left" border="0" width="100%" style="width:100%; border-collapse:collapse; border-spacing:0;">
            <tr>
              <th align="left">
                <span style="text-transform: uppercase; color:#B6B6B6; font-weight:normal; font-size:14px;">Analysis Start</span>
              </th>
              <th align="left">
                <span style="text-transform: uppercase; color:#B6B6B6;font-weight:normal; font-size:14px;">Analysis End</span>
              </th>
            </tr>
            <tr>
              <td style="font-size: 20px;">${startTime} ${timeZone}</td>
              <td style="font-size: 20px;">${endTime} ${timeZone}</td>
            </tr>
            </table>
          </td>
        </tr>

        <#if precision?has_content>
          <tr>
            <td style="padding: 0 24px;" colspan="2">
              <table align="left" border="0" width="100%" style="width:100%; border-collapse:collapse; border-spacing:0;">
                <tr>
                  <th align="left">
                  <span style="text-transform: uppercase; color:#B6B6B6; font-weight:normal; font-size:14px;">Precision</span>
                  </th>
                  <th align="left">
                  <span style="text-transform: uppercase; color:#B6B6B6;font-weight:normal; font-size:14px;">Recall</span>
                  </th>
                </tr>
                <tr>
                  <td style="font-size: 20px;">${precision}</td>
                  <td style="font-size: 20px;">${recall}</td>
                </tr>
              </table>
            </td>
          </tr>
        </#if>

        <#if cid?has_content>
          <tr>
              <td style="padding: 24px;" colspan="2" align="center">
                <a href="${anomalyDetails[0].anomalyURL}${anomalyDetails[0].anomalyId}" target="_blank"><img style="width: 70%;" src="cid:${cid}"\></a>
              </td>
          </tr>
        </#if>


      <#if rootAnomalyDetails?has_content>
        <tr>
          <td style="padding: 24px;" colspan="2">
            <table align="left" border="0" width="100%" style="width:100%; border-collapse:collapse; border-spacing:0; font-size: 14px;">
              <tr>
                <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Metric / Dimensions</th>
                <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Observed</th>
                <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Prediction</th>
                <#if containsSeasonal>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">WoW</th>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Wo2W</th>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Wo3W</th>
                </#if>
                <#if includeSummary>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Status</th>
                </#if>
              </tr>
              <#list rootAnomalyDetails as r>
                <tr style="border-top:1px solid #CFCFCF; border-bottom:1px solid #CFCFCF; background-color:#F5F5F5;">
                  <td style="padding:12px;"><a href="${r.anomalyURL}${r.anomalyId}" target="_blank" style="font-size: 16px; color: #33aada; font-weight: 600;">${r.metric}</a><br>

                    <#list r.dimensions as dimension>
                    ${dimension} <br>
                    </#list>
                    <#if r.issueType??>
                      issue type : ${r.issueType} <br>
                    </#if>


                  </td>
                  <td style="padding:12px;">
                    <span style="white-space: nowrap;">${r.currentVal}</span>
                  </td>
                  <td style="padding:12px;">
                    <span style="white-space: nowrap;">${r.baselineVal}</span><br>
                    <span style="font-size: 16px; color:
                      ${r.positiveLift?string('#398b18','#ee1620')};">${r.positiveLift?string('&#9650;','&#9660;')} ${r.lift}</span>
                  </td>
                  <#if containsSeasonal>
                    <td style="padding:12px;">
                      <span style="white-space: nowrap;">${r.wowValue}</span><br>
                      <span style="font-size: 16px; color:
                      ${r.positiveWoWLift?string('#398b18','#ee1620')};">${r.positiveWoWLift?string('&#9650;','&#9660;')} ${r.wowLift}</span>
                    </td>
                    <td style="padding:12px;">
                      <span style="white-space: nowrap;">${r.wo2wValue}</span><br>
                      <span style="font-size: 16px; color:
                      ${r.positiveWo2WLift?string('#398b18','#ee1620')};">${r.positiveWo2WLift?string('&#9650;','&#9660;')} ${r.wo2wLift}</span>
                    </td>
                    <td style="padding:12px;">
                      <span style="white-space: nowrap;">${r.wo3wValue}</span><br>
                      <span style="font-size: 16px; color:
                      ${r.positiveWo3WLift?string('#398b18','#ee1620')};">${r.positiveWo3WLift?string('&#9650;','&#9660;')} ${r.wo3wLift}</span>
                    </td>
                  </#if>
                  <#if includeSummary>
                    <td style="padding:12px;">${r.feedback}</td>
                  </#if>
                </tr>
              </#list>

            </table>
          </td>
        </tr>
      </#if>

        <#if leafAnomalyDetails?has_content>
          <tr>
            <td style="padding: 24px;" colspan="2">
              <#list leafAnomalyDetails?keys as key>
                <h4>${key}</h4>
              <table align="left" border="0" width="100%" style="width:100%; border-collapse:collapse; border-spacing:0; font-size: 14px;">
                <tr>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Metric / Dimensions</th>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Observed</th>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Prediction</th>
                  <#if containsSeasonal>
                    <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">WoW</th>
                    <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Wo2W</th>
                    <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Wo3W</th>
                  </#if>
                  <#if includeSummary>
                    <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Status</th>
                  </#if>
                </tr>
                <#list leafAnomalyDetails[key] as r>
                  <tr style="border-top:1px solid #CFCFCF; border-bottom:1px solid #CFCFCF; background-color:#F5F5F5;">
                    <td style="padding:12px;"><a href="${r.anomalyURL}${r.anomalyId}" target="_blank" style="font-size: 16px; color: #33aada; font-weight: 600;">${r.metric}</a><br>

                      <#list r.dimensions as dimension>
                        ${dimension} <br>
                      </#list>
                      <#if r.issueType??>
                        issue type : ${r.issueType} <br>
                      </#if>


                    </td>
                    <td style="padding:12px;">
                      <span style="white-space: nowrap;">${r.currentVal}</span>
                    </td>
                    <td style="padding:12px;">
                      <span style="white-space: nowrap;">${r.baselineVal}</span><br>
                      <span style="font-size: 16px; color:
                      ${r.positiveLift?string('#398b18','#ee1620')};">${r.positiveLift?string('&#9650;','&#9660;')} ${r.lift} (SWI: ${r.swi})</span>
                    </td>
                    <#if containsSeasonal>
                      <td style="padding:12px;">
                        <span style="white-space: nowrap;">${r.wowValue}</span><br>
                        <span style="font-size: 16px; color:
                        ${r.positiveWoWLift?string('#398b18','#ee1620')};">${r.positiveWoWLift?string('&#9650;','&#9660;')} ${r.wowLift}</span>
                      </td>
                      <td style="padding:12px;">
                        <span style="white-space: nowrap;">${r.wo2wValue}</span><br>
                        <span style="font-size: 16px; color:
                        ${r.positiveWo2WLift?string('#398b18','#ee1620')};">${r.positiveWo2WLift?string('&#9650;','&#9660;')} ${r.wo2wLift}</span>
                      </td>
                      <td style="padding:12px;">
                        <span style="white-space: nowrap;">${r.wo3wValue}</span><br>
                        <span style="font-size: 16px; color:
                        ${r.positiveWo3WLift?string('#398b18','#ee1620')};">${r.positiveWo3WLift?string('&#9650;','&#9660;')} ${r.wo3wLift}</span>
                      </td>
                    </#if>
                    <#if includeSummary>
                      <td style="padding:12px;">${r.feedback}</td>
                    </#if>
                  </tr>
                </#list>

              </table>
              </#list>
            </td>
          </tr>
        </#if>

        <tr>

          <#if holidays?has_content>
            <tr>
              <td style="padding: 24px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;" colspan="2" align="center">
                <p style="font-size:20px; line-height:24px; color:#1D1D1D; font-weight: 500; margin:0; padding:0;">Holidays</p>
                <table>
                <tr>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Holiday</th>
                  <th align="left" style="padding:12px; color:white; background-color: #0091CA; font-weight:600;">Impact Countries</th>
                </tr>
                <#list holidays as holiday>
                <tr>
                  <td style="padding:12px;">
                    <a href="https://www.google.com/search?q=${holiday.name}" style="text-decoration: none; color:#0073B1; font-size:14px; font-weight:bold; line-height:20px; margin-bottom: 0;">${holiday.name}</a>
                    <span style="font-size: 14px; color:#606060; line-height:20px;">(${holiday.startTime?number_to_date})</span>
                  </td>
                  <td style="padding:12px;">
                  <#if holiday.targetDimensionMap?has_content>
                    <#list holiday.targetDimensionMap?keys as dimension>
                      <span style="color: rgba(0,0,0,0.6); line-height:16px; font-size: 12px;">${holiday.targetDimensionMap[dimension]?join(",")}</span>
                    </#list>
                  </#if>
                  </td>
                </tr>
                </#list>
                </table>
              </td>
            </tr>
          </#if>
        </tr>

        <tr>
          <td colspan="2" style="border-bottom: 1px solid #E9E9E9;">
          </td>
        </tr>

        <tr>
          <td style="font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif; color: rgba(0,0,0,0.55); padding: 24px; font-size:14px;" colspan="2">
            <p style="margin-top:0;"> You are receiving this email because you have subscribed to ThirdEye Alert Service for <strong>'${alertConfigName}'</strong>. If you have any questions regarding this report, please email
              <a style="color: #33aada;" href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
            </p>
            <p style="margin-bottom:0; margin-top: 24px;">
              Thanks,<br>
              ThirdEye Team
            </p>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</body>
