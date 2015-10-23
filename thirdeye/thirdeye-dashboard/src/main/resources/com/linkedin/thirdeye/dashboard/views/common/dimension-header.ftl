<#-- n.b. expects "dimensions" and "dimensionAliases" variables to be assigned -->

<div class="dimension-combination">

    <button class="select-button uk-button" type="button" style="width: 225px;"><span>Change Dimension Query </span><i class="uk-icon-caret-down"></i></button>
    <div  class="hidden" style="width: 225px; z-index:100; position:absolute; top:30px; left:0; background-color: #f5f5f5; border: 1px solid #ccc; height: auto;">
        <ul class="multiselect-panel" style="z-index: 100; list-style-type: none; padding-left: 0px; margin-bottom: 0px;">
            <li class="multiselect-close"><a href="#"><i style="position: relative;
    left: 205px; color:#444;" class="uk-icon-close"></i></a></li>
        <#list (metricView.view.metricTables)!metricTables as metricTable>
            <#assign dimensions = metricTable.dimensionValues>
            <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
            <#list dimensions?keys as dimensionName>
                <#assign dimensionValue = dimensions[dimensionName]>
                <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                <li class="multiselect-optgroup-label" ><a href="#"  style="text-decoration: none; color: #444; background-image: -webkit-linear-gradient(top,#eee,#999);background-image: linear-gradient(to bottom,#eee,#999);">${dimensionDisplay}:</a></li>
                <li class="multiselect-optgroup hidden" style="text-align: left;">
                    <ul style="list-style-type: none; padding-left: 0px;">
                        <li class="option-label">
                                <input id="DIMENSIONINDEX_${dimensions[dimensionName]}" type="checkbox" type="checkbox" value="${dimensions[dimensionName]}"/><span>${dimensions[dimensionName]}</span></label>
                        </li>
                    </ul>
                </li>
            </#list>
        </#list>
        </ul>
    </div>


   <#-- <table>
        <tr>
            <td>Select Filters: </td>
    <#list (metricView.view.metricTables)!metricTables as metricTable>
         <#assign dimensions = metricTable.dimensionValues>
         <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
            <#list dimensions?keys as dimensionName>
                <#assign dimensionValue = dimensions[dimensionName]>
                <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                <td style="position:relative;">
                    <span>${dimensionDisplay}:</span><br>

                    <button type="button" class="dimension-selector uk-button">Select Values <i class="uk-icon-caret-down"></i> </button>
                    <div class="hidden" style="position:absolute; top:50px; left:0px; z-index:100; background-color: #f5f5f5; border: 1px solid #ccc; padding:5px;">
                        <ul style="list-style-type: none; padding-left:0; width:250px;">

                                <li style="overflow:hidden;">
                                    <input class="panel-metric-option" type="checkbox" value="${dimensionName}"/>${dimensions[dimensionName]}
                                </li>

                        </ul>
                    </div>

                </td>
            </#list>
    </#list>
        </tr>
    </table>
    <div class="uk-display-inline-block uk-margin-right">
        <button type="submit" class="time-input-form-submit uk-button uk-button-small uk-button-primary ">Go</button>
    </div>

   -->


</div>
