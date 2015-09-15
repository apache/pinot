<#-- n.b. expects "dimensions" and "dimensionAliases" variables to be assigned -->

<div class="dimension-combination">
    <div id="time-nav-buttons" class="uk-button-group">
        <button class="uk-button" id="time-nav-left">
            <i class="uk-icon-angle-left"></i>
        </button>
        <button class="uk-button" id="time-nav-right">
            <i class="uk-icon-angle-right"></i>
        </button>
    </div>

    <table>
        <tr>
            <td>Current Query: </td>

            <#list dimensions?keys as dimensionName>
                <#assign dimensionValue = dimensions[dimensionName]>
                <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                <td>
                    <#if dimensionValue == "*">
                        <span>${dimensionDisplay}:</span><br> ALL
                    <#elseif dimensionValue == "?">
                        <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span><br> OTHER</a>
                    <#else>
                        <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span><br> ${dimensions[dimensionName]}</a>
                    </#if>
                </td>
            </#list>
        </tr>
    </table>

</div>
