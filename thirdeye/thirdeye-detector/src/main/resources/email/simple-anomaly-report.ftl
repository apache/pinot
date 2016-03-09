<p>
    <a href="${visualizerLink}">${visualizerLink}</a>
</p>

<#if (anomalyResults?has_content)>

    <#list anomalyResults as r>
        <table>
            <tr>
                <th style="text-align: left">id</th>
                <td>${r.id} <a href="${idLink}${r.id?c}">(link)</a></td>
            </tr>
            <tr>
                <th style="text-align: left">function_id</th>
                <td>${r.functionId}</td>
            </tr>
            <tr>
                <th style="text-align: left">function_type</th>
                <td>${r.functionType}</td>
            </tr>
            <tr>
                <th style="text-align: left">function_properties</th>
                <td>${r.functionProperties}</td>
            </tr>
            <tr>
                <th style="text-align: left">dimensions</th>
                <td>${r.dimensions}</td>
            </tr>
            <tr>
                <th style="text-align: left">score</th>
                <td>${r.score}</td>
            </tr>
            <tr>
                <th style="text-align: left">weight</th>
                <td>${r.weight}</td>
            </tr>
            <tr>
                <th style="text-align: left">properties</th>
                <td>${r.properties}</td>
            </tr>
        </table>
        <hr/>
    </#list>

<#else>

    <p>No anomalies detected</p>

</#if>