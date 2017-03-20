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
      <p style="color: #737373; font-size: 14px;"> ThirdEye has detected
        <strong>${anomalyCount} </strong> ${(anomalyCount == 1)?string("anomaly", "anomalies")} for <strong>${datasets}</strong> from <strong>${startTime}</strong> to <strong>${endTime}</strong>.
      </p>
      <p style="color: #737373; font-size: 14px;">Below is a summary, please go <strong><a style="color:#0084bf;" href="${dashboardHost}/thirdeye#anomalies?anomaliesSearchMode=id&anomalyIds=${anomalyIds}">here</a></strong> for a detailed view.</p>
    </td>
  </tr>

  <#if anomalyDetails?has_content>
    <tr>
      <td style="padding: 24px;" colspan="2">
        <table align="center" border="0" width="100%" style="width:100%; border-collapse:collapse; border-spacing:0;margin-bottom:24px;font-size: 12px;">
          <tr>
            <th style="border:1px solid #CCC; padding: 8px;">Metric/<br>Dimension</th>
            <th style="border:1px solid #CCC; padding: 8px; width:18%;" colspan="18%">Duration</th>
            <th colspan="20%" style="border:1px solid #CCC; padding:0 8px; width:20%;">Details</th>
            <#if includeSummary>
              <th style="border:1px solid #CCC; padding: 8px;">Status</th>
            </#if>
            <th style="border:1px solid #CCC; padding: 8px;">Investigate</th>
          </tr>
          <#list anomalyDetails as r>
            <tr>
              <td style="border:1px solid #CCC; padding:0 8px;">${r.metric}<br> ${r.dimensions}</td>
              <td style="border:1px solid #CCC; padding:0 8px; width:18%;" colspan="18%">${r.duration}</td>
              <td colspan="20%" style="border:1px solid #CCC; padding:0 8px; width:20%;">
                <b>Change: </b><span style="color:
                ${r.positiveLift?string('#398b18','#ee1620')};">${r.lift}</span><br>
                <b>Current: </b>${r.currentVal} <br>
                <b>Baseline: </b>${r.baselineVal}
              </td>
              <#if includeSummary>
                <td style="border:1px solid #CCC; padding:0 8px;">${r.feedback}</td>
              </#if>
              <td style="border:1px solid #CCC; padding:0 8px;"><a href="${r.anomalyURL}${r.anomalyId}" target="_blank" style="color: white;font-weight: 600;background-color: #0084bf;font-size: 12px;padding: 0 8px;line-height: 20px;border-radius: 2px;cursor: pointer;display: inline-block;border: 1px solid transparent;text-decoration: none;">Investigate</a></td>
            </tr>
          </#list>

        </table>
      </td>
    </tr>
  </#if>

<tr>
  <td style="font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:14px; color: #737373;font-weight:300; text-align: center;" colspan="2">
    <p> You are receiving this email because you have subscribed to ThirdEye Alert Service for <strong>'${alertConfigName}'</strong>.<br>If you have any questions regarding this report, please email <br>
      <a href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
    </p>
    <p>
      Thanks,<br>
      ThirdEye Team
    </p>
  </td>
</tr>
</table>
