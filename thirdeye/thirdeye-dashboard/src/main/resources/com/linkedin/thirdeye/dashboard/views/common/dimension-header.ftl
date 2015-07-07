<#-- n.b. expects "dimensions" variable to be assigned -->

<div class="dimension-combination">
    <ul class="uk-breadcrumb">
        <#list dimensions?keys as dimensionName>
            <#assign dimensionValue = dimensions[dimensionName]>
            <li>
                <#if dimensionValue == "*">
                    ${dimensionName}:ALL
                <#elseif dimensionValue == "?">
                    <a href="#" class="dimension-link" dimension="${dimensionName}">${dimensionName}:OTHER</a>
                <#else>
                    <a href="#" class="dimension-link" dimension="${dimensionName}">${dimensionName}:${dimensions[dimensionName]}</a>
                </#if>
            </li>
        </#list>
    </ul>
</div>
