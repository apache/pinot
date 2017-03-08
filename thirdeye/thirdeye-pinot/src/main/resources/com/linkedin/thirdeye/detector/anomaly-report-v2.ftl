<html>
  <head>
  </head>
  <body>
    <table border="0" cellpadding="0" cellspacing="0"
           style="padding:0px; width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:15px;line-height:normal;margin:0 auto; padding:0px 0px 10px 0px; background-color: #fff;">
      <tr class="thirdeye-header" style="height:50px; background-color: #F3F6F8;"">
        <td align="left" style="padding: 10px 24px;height:50px;" colspan="2">
          <img width="45" height="35" alt="logo" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACsAAAAiCAQAAABsW+iDAAAA3klEQVRIx83Wuw3CMBAG4BshaahZIBUjsAKNB8gopqaiTpsRGMEjsANI3ADI+CElcTgKuLuQsyIrUvTJ+n1WDL7yJ8+pLggwH8BEM0ywEqXEbpdfLfZYA2CNvSCLDoZCJ8faCWt12HbtISht2Z+OA97QpXH9kh2zLd8D9cR2knyNZwnWxszLmvXKLyxdRbcIsgcBNgQRt+uCuzFhNotH6tDwWafMvn/FYB93FbZo0cXZxps0Gkk2opkPsBxr0rPPszRr/EaDBenVfsqW/XegO2F9dzCC7XQuohUTJq/NL1/k/oovlOCIAAAAAElFTkSuQmCC" style="vertical-align: middle; display: inline-block;">
          <span style="color: #737373;font-size: 15px;display: inline-block;vertical-align: middle;">THIRDEYE</span>
        </td>
      </tr>
      <tr>
        <td style="padding: 0 24px;" colspan="2">
          <p style="font-size: 20px; font-weight: 600;">Hi,</p>
          <p style="color: #737373; font-size: 14px;">You are receiving this email because you have subscribed to ThirdEye Alert Service for <strong>'${alertConfigName}'</strong>.</p>
          <p style="color: #737373; font-size: 14px;">Down below, you will find the anomalies that were detected.</p>
        </td>
      </tr>
      <tr>
        <td colspan="2" style="border-bottom: 1px solid #CCC"></td>
      </tr>
      <#if anomalyDetails?has_content>
        <#list anomalyDetails as r>
          <tr>
            <td style="font-size: 19px; font-weight:600; padding: 24px;">${r.metric}</td>
            <td style="text-align: right; padding: 24px"><a href="${r.anomalyURL}" target="_blank" style="color: white;font-weight: 600;background-color: #0084bf;font-size: 17px;padding: 0 16px;line-height: 32px;border-radius: 2px;cursor: pointer;display: inline-block;border: 1px solid transparent;text-decoration: none;">Investigate</a></td>
          </tr>

          <tr>
            <td style="padding: 24px;" colspan="2">
              <div style="height: 100px; width: 100%; background-color: black; color: white">Graph placeholder</div>
            </td>
          </tr>

          <tr>
            <td style="padding: 24px;" colspan="2">
              <table style="width:100%; text-align: center; padding:0; margin:0; border-collapse: collapse; border: 1px solid #CCC" border="0" align="center">
                <tr>
                  <td colspan="33%" style="border: 1px solid #CCC; padding: 14px;font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Change</div></td>
                  <td colspan="33%" style="border: 1px solid #CCC; padding: 14px;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Current</div></td>
                  <td colspan="33%" style="border: 1px solid #CCC; padding: 14px;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Baseline</div></td>
                </tr>
                <tr>
                  <td colspan="50%" style="border: 1px solid #CCC; padding: 14px;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Dimension</div></td>
                  <td colspan="50%" style="border: 1px solid #CCC; padding: 14px;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Function</div></td>
                </tr>
                <tr>
                  <td colspan="50%" style="border: 1px solid #CCC; padding: 14px;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Duration</div></td>
                  <td colspan="50%" style="border: 1px solid #CCC; padding: 14px;"><div style="color: #737373;">placeholder</div><div style="font-weight:600;">Status</div></td>
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
  </body>
</html>



