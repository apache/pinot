<#-- n.b. expects "dimensions" and "dimensionAliases" variables to be assigned -->

<div class="dimension-combination">
    <!--<div id="time-nav-buttons" class="uk-button-group">
        <button class="uk-button" id="time-nav-left">
            <i class="uk-icon-angle-left"></i>
        </button>
        <button class="uk-button" id="time-nav-right">
            <i class="uk-icon-angle-right"></i>
        </button>
    </div>-->

    <table>
        <tr>
            <td>Current Query: </td>
    <#list (metricView.view.metricTables)!metricTables as metricTable>
         <#assign dimensions = metricTable.dimensionValues>
         <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
            <#list dimensions?keys as dimensionName>
                <#assign dimensionValue = dimensions[dimensionName]>
                <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                <td style="position:relative;">
                    <span>${dimensionDisplay}:</span><br>
                    <#--<#if dimensionValue == "*">
                        <span>${dimensionDisplay}:</span><br> ALL
                    <#elseif dimensionValue == "?">
                        <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span><br> OTHER</a>
                    <#else>
                        <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span><br> ${dimensions[dimensionName]}</a>
                    </#if>-->
                    <button type="button" class="dimension-selector uk-button">Select Values <i class="uk-icon-caret-down"></i> </button>
                    <div class="hidden" style="position:absolute; top:50px; left:0px; z-index:100; background-color: #f5f5f5; border: 1px solid #ccc; padding:5px;">
                        <ul style="list-style-type: none; padding-left:0; width:250px;">
                            <#--<#list collectionSchema.dimensions as dim>-->
                                <li style="overflow:hidden;">
                                    <input class="panel-metric" type="checkbox" value="${dimensionName}"/>${dimensions[dimensionName]}
                                </li>
                            <#--</#list>-->
                        </ul>
                    </div>

                </td>
            </#list>
    </#list>
        </tr>
    </table>
</div>
