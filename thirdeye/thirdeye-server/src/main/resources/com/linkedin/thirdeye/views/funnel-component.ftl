<#list funnels as funnel>
    <table class="funnel">
        <#list funnel.rows as row>
            <tr>
                <td class="funnel-metric-name">${row.metric.name}</td>

                <td class="funnel-conversion">
                    <span class="baseline-conversion">${(row.startRatio * 100)?string["0.00"]}%</span>
                    /
                    ${(row.endRatio * 100)?string["0.00"]}%
                </td>

                <#assign diff = (row.endRatio - row.startRatio) * 100>

                <#if (diff >= 0)>
                    <#assign quotientWidth = (row.startRatio * 100)>
                    <#assign diffClass = "funnel-increase">
                <#elseif (diff == 0)>
                    <#assign quotientWidth = (row.startRatio * 100)>
                    <#assign diffClass = "funnel-same">
                <#else>
                    <#assign quotientWidth = (row.endRatio * 100)>
                    <#assign diffClass = "funnel-decrease">
                </#if>

                <td class="${diffClass} funnel-conversion">${diff?string["0.00"]}%</td>

                <td width="50%">
                    <table width="100%" class="funnel-chart">
                        <td class="funnel-quotient" width="${quotientWidth?string["0"]}%"></td>
                        <td class="${diffClass}" width="${(diff?abs)?string["0"]}%"></td>
                        <td class="funnel-remainder" width="${(100 - quotientWidth - diff)?string["0"]}%"></td>
                    </table>
                </td>
            </tr>
        </#list>
    </table>
</#list>

<style>
.funnel-metric-name, .funnel-conversion {
    padding: 5px;
}

.funnel-increase {
    color: green;
}

.funnel-same {
    color: black;
}

.funnel-decrease {
    color: red;
}

.funnel-chart {
    border-collapse: collapse;
}

.funnel-chart td {
    padding: 0px;
}

.funnel-chart .funnel-increase {
    background-color: rgb(138, 252, 136);
    border-right: 1px solid black;
}

.funnel-chart .funnel-same {
    background-color: black;
}

.funnel-chart .funnel-decrease {
    background-color: rgb(252, 136, 138);
    border-left: 1px solid black;
}

.baseline-conversion {
    color: grey;
}

.funnel-quotient {
    background-color: #eee;
    height: 40px;
}
</style>