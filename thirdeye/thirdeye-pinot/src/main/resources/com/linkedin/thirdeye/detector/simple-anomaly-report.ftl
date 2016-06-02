<p>
    Hello,
    <br>
    You are receiving this email because you have subscribed to ThirdEye Anomaly detection service for '${collection}:${metric}'.<br/>
    ThirdEye has analyzed your dataset for time range ${dateFormat(startTime)} to ${dateFormat(endTime)} (${timeZone}) and has detected <b>${anomalyCount} ${(anomalyCount == 1)?string("anomaly", "anomalies")}.</b> </br>
    <#if (anomalyCount > 0)>
     Below is the full list of anomalies detected during this time period.
    </#if>
</p>    
<p>
    <img id="Chart" src="cid:${embeddedChart}">
</p>
<#if (groupedAnomalyResults?has_content)>
    <table border="1" style="width:100%">
        <tr>
            <th>Dimensions</th>
            <th>No.</th>
            <th>Time (${timeZone})</th>
            <#-- <th>End</th> -->
            <th>Reason</th>
            <th>Function</th>
        </tr>
        <#assign anomalySequenceIndex = 1> 
        <#list groupedAnomalyResults?keys as dimensionKey>
            <#assign results = groupedAnomalyResults[dimensionKey]>
            <#assign dimensionStr = assignedDimensions(dimensionKey)>
            <#list results as r>
                <tr>
                    <#if r == results?first>
                        <td rowspan="${results?size}">${dimensionStr}</td>
                    </#if>
                    <td style="text-align:center">
                        <#-- <a href="${anomalyEndpoint}${r.id?c}" target="_blank"> -->
                        ${anomalySequenceIndex?c}
                        <#-- </a> -->
                        </td>
                    <td>
                        <#-- Assuming start time is always present -->
                        <#if r.endTimeUtc??>
                            ${dateFormat(r.startTimeUtc)} to ${dateFormat(r.endTimeUtc)}
                        <#else>
                            ${dateFormat(r.startTimeUtc)}
                        </#if>
                    </td>
                    <#-- <td style="text-align:center">${r.endDate!"N/A"}</td> -->
                    <td>${r.message!"N/A"}</td>
                    <td>
                        <#-- <a href="${functionEndpoint}${r.functionId?c}" target="_blank"> -->
                        ${r.functionType} (${r.functionId?c})
                        <#-- </a> -->
                    </td>
                </tr>
                <#assign anomalySequenceIndex = anomalySequenceIndex + 1>
            </#list>
        </#list>
    </table>
    <hr/>
</#if>
<br/>

<!--Go to <a href="${dashboardHost}/dashboard#view=anomalies&dataset=${collection}&rand=896&compareMode=WoW&aggTimeGranularity=${windowUnit}&currentStart=${startTime?c}&currentEnd=${endTime?c}&metrics=${metric}&filters=${filters}" target="_top">ThirdEye Anomalies Dashboard</a>-->

<br/>

If you have any questions regarding this report, please email <a href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
<br/>
Report generated at: ${dateFormat(reportGenerationTimeMillis)}
<br/>
<br/>
Thanks,<br/>
ThirdEye Team
