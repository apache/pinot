<#-- n.b. expects "dimensions" variable to be assigned -->

<div class="dimension-combination">
    <ul class="uk-breadcrumb">
        <#list dimensions?keys as dimensionName>
            <#assign dimensionValue = dimensions[dimensionName]>
            <li>
                <#if dimensionValue != "*">
                    <a href="#" class="dimension-link" dimension="${dimensionName}">${dimensionName}:${dimensions[dimensionName]}</a>
                <#else>
                    ${dimensionName}:${dimensions[dimensionName]}
                </#if>
            </li>
        </#list>
    </ul>
</div>
