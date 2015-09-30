<#if (anomalyResults?has_content)>

    <#list anomalyResults as anomalyResult>
        <dl>
            <dt>id</dt>
            <dd>${anomalyResult.id}</dd>
            <dt>function_id</dt>
            <dd>${anomalyResult.function_id}</dd>
            <dt>function_type</dt>
            <dd>${anomalyResult.function_type}</dd>
            <dt>function_properties</dt>
            <dd>${anomalyResult.function_properties}</dd>
            <dt>dimensions</dt>
            <dd>${anomalyResult.dimensions}</dd>
            <dt>score</dt>
            <dd>${anomalyResult.score}</dd>
            <dt>weight</dt>
            <dd>${anomalyResult.weight}</dd>
            <dt>properties</dt>
            <dd>${anomalyResult.properties}</dd>
        </dl>
    </#list>

<#else>

    <p>No anomalies detected</p>

</#if>