<table align="center" border="0" cellspacing ="0" width="100%" bgcolor="#edf0f3" style="background-color: #edf0f3; table-layout:fixed">
  <tbody>
    <tr>
    <td align="center">
      <center style="width:100%">

        <table border="0" cellpadding="0" cellspacing="0"
               style="padding:0px; width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:15px;line-height:normal;margin:0 auto; padding:0px 0px 10px 0px; background-color: #fff; margin: 0 auto; max-width: 512px;">
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
                Analysis Start: ${startTime?date}<br/>
                Analysis End: ${endTime?date}
              </p>
            </td>
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
                      <div style="color: #737373">New Trend</div>
                      <div style="color: #737373">${newTrendCount}</div>
                    </td>
                  </tr>
                </table>
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding: 0 24px;" colspan="2">
              <p style="color: #737373; font-size: 14px;">Down below, you will find the anomalies that were detected.</p>
            </td>
          </tr>

          <tr>
            <td colspan="2" style="border-bottom: 1px solid #CCC"></td>
          </tr>
          <#if anomalyDetails?has_content>
            <#list anomalyDetails as r>
              <tr>
                <td style="font-size: 19px; font-weight:600; padding: 24px;">#${r.anomalyId} ${r.metric}</td>
                <td style="text-align: right; padding: 24px"><a href="${r.anomalyURL}${r.anomalyId}" target="_blank" style="color: white;font-weight: 600;background-color: #0084bf;font-size: 17px;padding: 0 16px;line-height: 32px;border-radius: 2px;cursor: pointer;display: inline-block;border: 1px solid transparent;text-decoration: none;">Investigate</a></td>
              </tr>

      <!--          <tr>
                <td style="padding: 24px;" colspan="2">
                  <div style="height: 100px; width: 100%; background-color: black; color: white">Graph placeholder</div>
                </td>
              </tr> -->
              <tr>
                <td style="padding: 24px;" colspan="2">
                  <table border="0"  width="100%" align="center" style="width:100%; text-align: center; padding:0; margin:0; border-collapse: collapse; border: 1px solid #CCC">
                    <tr>
                      <td colspan="33%" style="border: 1px solid #CCC; padding: 14px;font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;"><div style="color: #737373;">${r.lift}</div><div style="font-weight:600;">Change</div></td>
                      <td colspan="33%" style="border: 1px solid #CCC; width:33%; padding: 14px;"><div style="color: #737373;">${r.currentVal}</div><div style="font-weight:600;">Current Avg</div></td>
                      <td colspan="33%" style="border: 1px solid #CCC; width:33%; padding: 14px;"><div style="color: #737373;">${r.baselineVal}</div><div style="font-weight:600;">Baseline Avg</div></td>
                    </tr>
                    <tr>
                      <td colspan="50%" style="border: 1px solid #CCC; width:50%; padding: 14px;"><div style="color: #737373;">${r.dimensions}</div><div style="font-weight:600;">Dimension</div></td>
                      <td colspan="50%" style="border: 1px solid #CCC; width:50%; padding: 14px;"><div style="color: #737373;">${r.function}</div><div style="font-weight:600;">Function</div></td>
                    </tr>
                    <tr>
                      <td colspan="50%" style="border: 1px solid #CCC; width:50%; padding: 14px;"><div style="color: #737373;">${r.duration} hours</div><div style="font-weight:600;">Duration</div></td>
                      <td colspan="50%" style="border: 1px solid #CCC; width:50%; padding: 14px;"><div style="color: #737373;">${r.feedback}</div><div style="font-weight:600;">Status</div></td>
                    </tr>
                  </table>
                </td>
              </tr>

              <tr>
                <td colspan="2" style="border-bottom: 1px solid #CCC"></td>
              </tr>
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
      </center>
    </td>
  </tr>
  </tbody>
</table>




