<#-- n.b. expects "dimensions" and "dimensionAliases" variables to be assigned -->

<div class="dimension-combination">
    <h2>Current Query:</h2>
    <ul class="uk-breadcrumb">
        <#list dimensions?keys as dimensionName>
            <#assign dimensionValue = dimensions[dimensionName]>
            <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
            <li>
                <#if dimensionValue == "*">
                    ${dimensionDisplay}: ALL
                <#elseif dimensionValue == "?">
                    <a href="#" class="dimension-link" dimension="${dimensionName}">${dimensionDisplay}: OTHER</a>
                <#else>
                    <a href="#" class="dimension-link" dimension="${dimensionName}">${dimensionDisplay}: ${dimensions[dimensionName]}</a>
                </#if>
            </li>
        </#list>
    </ul>
</div>
