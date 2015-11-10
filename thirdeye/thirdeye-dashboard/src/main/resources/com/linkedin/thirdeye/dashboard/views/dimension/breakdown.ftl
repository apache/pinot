<script src="/assets/js/thirdeye.breakdown.js"></script>

<div id="dimension-contributor-area" width="100%">
    <#list dimensionView.view.metrics as metric>
        <#list dimensionView.view.dimensions as dimension>
            <#assign dimTableTotalRow=dimensionView.view.metricTotalTable[metric]>
            <#assign dimTable=dimensionView.view.getDimensionValueTable(metric,dimension)>
            <#assign dimensionDisplay = dimensionAliases[dimension]!dimension>

                <div class="metric-section-wrapper" rel="${metric}">
                <div class="section-wrapper" rel="${dimension}">

                    <h2>${metric}</h2>
                    <table id='contributors-view-${metric}' class="uk-table contributors-table fixed-table-layout" cell-spacing="0" width="100%">

                        <thead>
                        </thead>
                        <tbody>
                            <!-- First time row-->
                            <@timeRow cells=dimTableTotalRow.rows/>
                            <@tableRowTotal cells=dimTableTotalRow.rows class="hourly-values"/>
                            <@tableRowTotal cells=dimTableTotalRow.cumulativeRows class="cumulative-values hidden"/>

                            <!-- Divider row -->
                            <tr class="divider-row">
                                <td colspan="5"><h3>${dimension}</h3>
                                </td>
                                <#--<#list 0..(dimTableTotalRow.rows?size) as x>
                                    <td></td>
                                </#list>-->
                            </tr>

                            <!-- Second time row -->
                            <@timeRow cells=dimTableTotalRow.rows/>
                            <@tableRowSum class="hourly-values sum-row"/>
                            <@tableRowSum class="cumulative-values sum-row hidden"/>

                            <#list dimTable?keys as dimensionValue>
                                <#assign rows=dimTable[dimensionValue].rows>
                                <#assign cumulativeRows=dimTable[dimensionValue].cumulativeRows>
                                <!-- hourly values -->
                                <@tableRow dimension=dimensionDisplay dimensionValue=dimensionValue cells=rows class="hourly-values heat-map-row"/>

                                <#-- normal + cumulative rows are currently interweaved. If they need to be separated, add a second list loop-->
                                <!-- cumulative values -->
                                <@tableRow dimension=dimensionDisplay dimensionValue=dimensionValue cells=cumulativeRows class="cumulative-values heat-map-row hidden"/>
                            </#list>
                        </tbody>
                    </table>
                </div>  <!-- end of dimension wrapper -->
            </div>  <!-- end of metric wrapper -->
        </#list>
    </#list>
    <#macro timeRow cells>
       <tr>

            <td class="contributors-table-date" colspan="2" currentUTC="${cells[0].currentTime}">${cells[0].currentTime}</td>
            <#list cells as cell>
            <#-- TODO properly display time in timezone-->
                <td class="contributors-table-time" currentUTC="${cell.currentTime}">${cell.currentTime}</td>
            </#list>
        </tr>
    </#macro>

    <#macro tableRowTotal cells class>
        <tr class="${class}">
            <td colspan="2" class="divider-cell"></td>
            <#list cells as cell>
                <@timeBucketCell cell=cell/>
            </#list>
        </tr>
    </#macro>
    <#macro tableRow dimension dimensionValue cells class>
        <tr class="${class} data-row" dimension="${dimension}">
            <td class="checkbox-cell" style="width:15px;"><input value="1" type="checkbox" checked></td>
            <td class="dimension dimension-cell hidden">${dimension}</td>
            <#assign dimensionValueDisplay=dimensionValue?html>
            <#if dimensionValue=="">
                <#assign dimensionValueDisplay="UNKNOWN">
            <#elseif dimensionValue=="?">
                <#assign dimensionValueDisplay="OTHER">
            </#if>
            <td class="dimension-value-cell" style="width:100px; word-wrap: break-word;" >${dimensionValueDisplay}</td>
            <#list cells as cell>
                <@timeBucketCell cell=cell/>
            </#list>
        </tr>
    </#macro>


    <#macro timeBucketCell cell>
        <td class="details-cell hidden">
            <#if cell.baseline?? && cell.baseline[0]??>
                ${cell.baseline[0]}
            <#else>
                N/A
            </#if>
        </td>
        <td class="details-cell hidden">
            <#if cell.current?? && cell.current[0]??>
                ${cell.current[0]}
            <#else>
                N/A
            </#if>
        </td>
        <td class="heat-map-cell"
                value="
                <#if cell.ratio?? && cell.ratio[0]??>
                ${cell.ratio[0]}
            <#else>
                N/A
            </#if>
                "
                >
            <#if cell.ratio?? && cell.ratio[0]??>
                ${(cell.ratio[0] * 100)?string["0.0"]}%
            <#else>
                N/A
            </#if>
        </td>
    </#macro>

    <#macro tableRowSum class>
        <tr class="${class}">
            <th colspan="2">Total:</th>
            <#list 1..(dimTableTotalRow.rows?size) as columnIndex>
                <th class="details-cell hidden"></th>
                <th class="details-cell hidden"></th>
                <th class="heat-map-cell"></th>
            </#list>
        </tr>
    </#macro>

</div>
