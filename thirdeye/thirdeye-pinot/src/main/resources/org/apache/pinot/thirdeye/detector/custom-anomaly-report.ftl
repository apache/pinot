<table border="0" cellpadding="0" cellspacing="0"
       style="padding:0px; width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:12px;line-height:normal;margin:0 auto; padding:0px 0px 10px 0px; background-color: #fff;">
  <tr style="height:50px;background: #000">
    <td align="left" style="padding: 10px;height:50px;">
      <div style="height: 35px;width: 45px;display: inline-block; background-image: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACsAAAAiCAQAAABsW+iDAAAA3klEQVRIx83Wuw3CMBAG4BshaahZIBUjsAKNB8gopqaiTpsRGMEjsANI3ADI+CElcTgKuLuQsyIrUvTJ+n1WDL7yJ8+pLggwH8BEM0ywEqXEbpdfLfZYA2CNvSCLDoZCJ8faCWt12HbtISht2Z+OA97QpXH9kh2zLd8D9cR2knyNZwnWxszLmvXKLyxdRbcIsgcBNgQRt+uCuzFhNotH6tDwWafMvn/FYB93FbZo0cXZxps0Gkk2opkPsBxr0rPPszRr/EaDBenVfsqW/XegO2F9dzCC7XQuohUTJq/NL1/k/oovlOCIAAAAAElFTkSuQmCC); background-position: 0 0;
            background-repeat: no-repeat; "></div>
      <span
          style="width:200px;color:#fff;font-size:20px;display:inline;position:relative;top:-3px;font-weight:200;font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;">ThirdEye</span>
    </td>
  </tr>
  <tr>
    <td style="min-height: 30px; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:16px;font-weight:300; width:100%;display:inline;">
      <p>Hello,</p>
      <p>You are receiving this email because you have subscribed to ThirdEye Alert Service : '${alertConfigName}'.<br/></p>
  </td>
  </tr>
  <tr>
    <td style="min-height: 30px; padding:10px 15px; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:16px;font-weight:300; width:100%;display:inline;">
      <br/>Analysis Start : ${startTime?date}
      <br/>Analysis End : ${endTime?date}
    </td>
  </tr>
<#if includeSummary>
<tr>
    <td>
      <h3>Summary : </h3>
      <table align="left" border="1" cellpadding="4px"
             style="width:100%;border-collapse: collapse; border-spacing: 0 margin-bottom: 15px; border-color:#ddd;">
        <tr>
          <th>Anomalies</th>
          <th>Metrics</th>
          <th>Notified</th>
          <th>Feedback Received</th>
          <th>True Alert</th>
          <th>False Alert</th>
          <th>New Trend</th>
        </tr>
        <tr>
          <td>${anomalyCount}</td>
          <td>${metricsCount}</td>
          <td>${notifiedCount}</td>
          <td>${feedbackCount}</td>
          <td>${trueAlertCount}</td>
          <td>${falseAlertCount}</td>
          <td>${newTrendCount}</td>
        </tr>
      </table>
    </td>
  </tr>
</#if>
<#if anomalyDetails?has_content>
  <tr>
    <td>
      <h3>Details : </h3>
      <table align="left" border="1" cellpadding="4px"
             style="width:100%;border-collapse: collapse; border-spacing: 0 margin-bottom: 15px; border-color:#ddd;">
        <tr>
          <th>Metric</th>
          <th>Anomaly Start Time</th>
          <th>Window Size (hours)</th>
          <th>Lift</th>
          <#if includeSummary>
            <th>Feedback</th>
          </#if>
          <th>AnomalyId</th>
        </tr>
        <#list anomalyDetails as r>
          <tr>
            <td>${r.metric}</td>
            <td>${r.startDateTime}</td>
            <td>${r.windowSize}</td>
            <td>${r.lift}</td>
            <#if includeSummary>
            <td>${r.feedback}</td>
            </#if>
            <td><a href="${r.anomalyURL}" target="_blank">${r.anomalyId?string}</a></td>
          </tr>
        </#list>
      </table>
    </td>
  </tr>
</#if>
  <tr>
    <td style="font-family:font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:16px;font-weight:300; width:100%;display:inline;">
      <br/>
      <hr/>
      <p>If you have any questions regarding this report, please email <a
          href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a></p>
      <p> Report generated at: ${dateFormat(reportGenerationTimeMillis)}</p>
      <p>
        Thanks,<br>
        ThirdEye Team
      </p>
    </td>
  </tr>
</table>
