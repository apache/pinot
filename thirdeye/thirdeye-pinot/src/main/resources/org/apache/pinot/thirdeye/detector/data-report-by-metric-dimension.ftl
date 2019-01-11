<table border="0" cellpadding="0" cellspacing="0"
       style="padding:0px; width:100%; font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:12px;line-height:normal;margin:0 auto; padding:0px 0px 10px 0px; background-color: #fff;">
  <tr style="height:50px;background: #000">
    <td align="left" style="padding: 10px;height:50px;">
      <div style="height: 35px;width: 45px;display: inline-block; background-image: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACsAAAAiCAQAAABsW+iDAAAA3klEQVRIx83Wuw3CMBAG4BshaahZIBUjsAKNB8gopqaiTpsRGMEjsANI3ADI+CElcTgKuLuQsyIrUvTJ+n1WDL7yJ8+pLggwH8BEM0ywEqXEbpdfLfZYA2CNvSCLDoZCJ8faCWt12HbtISht2Z+OA97QpXH9kh2zLd8D9cR2knyNZwnWxszLmvXKLyxdRbcIsgcBNgQRt+uCuzFhNotH6tDwWafMvn/FYB93FbZo0cXZxps0Gkk2opkPsBxr0rPPszRr/EaDBenVfsqW/XegO2F9dzCC7XQuohUTJq/NL1/k/oovlOCIAAAAAElFTkSuQmCC); background-position: 0 0;
            background-repeat: no-repeat; "></div>
      <span
          style="width:200px;color:#fff;font-size:20px;display:inline;position:relative;top:-3px;font-weight:200;font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;">ThirdEye Data Report</span>
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
        <p>${reportCount} - <a
            href="${dashboardHost}/app/#/rootcause?metricId=${metricReport.metricId?c}">
        ${metricReport.metricName} by ${metricReport.dimensionName}</a>  (baseline : ${metricReport.compareMode})
        </p>
      </b></td>
    </tr>
    <#assign subDimensionValueMap = metricReport.subDimensionValueMap >
    <tr>
      <td>
        <table align="left" border="1"
               style="width:100%;border-collapse: collapse; border-spacing: 0 margin-bottom:15px;border-color:#ddd;"
               cellspacing="0px" cellpadding="4px">
          <tr>
            <td style="background-color: #e0e0eb;">${metricReport.dimensionName}</td>
            <td style="background-color: #e0e0eb;">Share</td>
            <td style="background-color: #e0e0eb;">Total</td>
            <#assign itrCount = 1 >
            <#list subDimensionValueMap?keys as groupByDimension>
              <#assign timeBucketValueMap = subDimensionValueMap[groupByDimension]>
              <#if itrCount == 1>
                <#list timeBucketValueMap?keys as timeBucket>
                  <td style="background-color: #e0e0eb;">
                  ${timeBucket?number?number_to_time?string("HH:mm")}
                  </td>
                </#list>
              </#if>
              <#assign itrCount = itrCount + 1>
            </#list>
          </tr>
          <#list subDimensionValueMap?keys as dimensionKey>
            <tr>
              <td style="background-color: #e0e0eb;">
              ${dimensionKey}
              </td>
              <#assign value = metricReport.subDimensionShareValueMap[dimensionKey]?number >
              <td> ${value}%</td>

              <#assign value = metricReport.subDimensionTotalValueMap[dimensionKey]?number >
              <td
                <#if (value> 35) > style="background-color: #3333ff;" </#if>
                <#if (value > 15) > style="background-color: #8080ff;" </#if>
                <#if (value > 5) > style="background-color: #e6e6ff;" </#if>
                <#if (value < -35) > style="background-color: #ff0080;" </#if>
                <#if (value < -15) > style="background-color: #ff80bf;" </#if>
                <#if (value < -5) > style="background-color: #ffe6f2;" </#if>
              > ${value}%</td>

              <#assign timevalmap = subDimensionValueMap[dimensionKey] >
              <#list timevalmap?keys as timebucketkey>
                <#assign value = timevalmap[timebucketkey]?number >
                <td
                  <#if (value> 35) > style="background-color: #3333ff;" </#if>
                  <#if (value > 15) > style="background-color: #8080ff;" </#if>
                  <#if (value > 5) > style="background-color: #e6e6ff;" </#if>
                  <#if (value < -35) > style="background-color: #ff0080;" </#if>
                  <#if (value < -15) > style="background-color: #ff80bf;" </#if>
                  <#if (value < -5) > style="background-color: #ffe6f2;" </#if>
                > ${value}%</td>
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
    <td style="font-family:font-family: 'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif;font-size:16px;font-weight:300; width:100%;display:inline;">
      <hr/>
      <br/>
      <p>If you have any questions regarding this report, please email <a
          href="mailto:'${contactEmail}'" target="_top">${contactEmail}</a></p>
      <p>
        Thanks,<br>
        ThirdEye Team
      </p>
    </td>
  </tr>
</table>
