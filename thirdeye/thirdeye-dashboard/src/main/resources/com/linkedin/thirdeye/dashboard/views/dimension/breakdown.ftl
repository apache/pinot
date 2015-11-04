<div id="dimension-contributor-area">
    <#assign dimTableTotalRow=dimensionView.view.metricTotalTable>
    <#list dimensionView.view.dimensions as dimension>
        <#assign dimTable=dimensionView.view.dimensionTables[dimension]>
        <#list dimensionView.view.metrics as metric>
            <div class="metric-section-wrapper" rel="${metric}">
                <div class="section-wrapper" rel="${dimension}">
                    <table id='contributors-view-${metric}' class="contributors-table" cell-spacing="0" width="100%">

                        <thead>
                        </thead>
                        <tbody>
                            <!-- First time row-->
                            <@timeRow cells=dimTableTotalRow.rows/>

                            <@tableRowTotal cells=dimTableTotalRow.rows metric_index=metric_index class=""/>
                            <@tableRowTotal cells=dimTableTotalRow.cumulativeRows metric_index=metric_index class="cumulative hidden"/>
                            <!-- Divider row -->
                            <tr class="divider-row">
                                <td>${metric}</td>
                                <td></td>
                                <td></td>
                                <#list 0..(dimTableTotalRow.rows?size) as x>
                                    <td></td>
                                </#list>
                            </tr>

                            <!-- Second time row -->
                            <@timeRow cells=dimTableTotalRow.rows/>

                            <#list dimTable?keys as dimensionValue>
                                <#assign dimensionDisplay = dimensionAliases[dimension]!dimension>

                                <!-- hourly values -->
                                <@tableRow dimension=dimensionDisplay dimensionValue=dimensionValue cells=dimTable[dimensionValue].rows metric_index=metric_index class="hourly-values"/>

                                <#-- normal + cumulative rows are currently interweaved. If they need to be separated, add a second list loop-->
                                <!-- cumulative values -->
                                <@tableRow dimension=dimensionDisplay dimensionValue=dimensionValue cells=dimTable[dimensionValue].cumulativeRows metric_index=metric_index class="cumulative-values hidden"/>
                            </#list>
                        </tbody>
                        <tfoot>
                            <tr>
                                <th>Total:</th>
                                <th></th>
                                <th></th>
                                <#list 0..(dimTableTotalRow.rows?size) as columnIndex>
                                    <th></th>
                                </#list>
                            </tr>
                        </tfoot>
                    </table>
                </div>  <!-- end of dimension wrapper -->
            </div>  <!-- end of metric wrapper -->
            <br/>
        </#list>
    </#list>
    <#macro timeRow cells>
       <tr>
            <td class=""></td>
            <td class=""></td>
            <td class=""></td>
            <#list cells as cell>
            <#-- TODO properly display time in timezone-->
                <td colspan="3">${cell.currentTime}</td>
            </#list>
        </tr>
    </#macro>

    <#macro tableRowTotal cells metric_index class>
        <tr class="${class}">
            <td class="checkbox"></td>
            <td class="dimension"></td>
            <td class="dimension-value"></td>
            <#list cells as cell>
                <@timeBucketCell cell=cell metric_index=metric_index/>
            </#list>
        </tr>
    </#macro>
    <#macro tableRow dimension dimensionValue cells metric_index class>
        <tr class="${class}">
            <td><input value="1" type="checkbox"></td>
            <td class="dimension">${dimension}</td>
            <#assign dimensionValueDisplay=dimensionValue?html>
            <#if dimensionValue=="">
                <#assign dimensionValueDisplay="UNKNOWN">
            <#elseif dimensionValue=="?">
                <#assign dimensionValueDisplay="OTHER">
            </#if>
            <td class="dimension-value">${dimensionValueDisplay}</td>
            <#list cells as cell>
                <@timeBucketCell cell=cell metric_index=metric_index/>
            </#list>
        </tr>
    </#macro>

    <#macro timeBucketCell cell metric_index>
        <td>
            <#if cell.baseline?? && cell.baseline[metric_index]??>
                ${cell.baseline[metric_index]}
            <#else>
                N/A
            </#if>
        </td>
        <td>
            <#if cell.current?? && cell.current[metric_index]??>
                ${cell.current[metric_index]}
            <#else>
                N/A
            </#if>
        </td>
        <td>
            <#if cell.ratio?? && cell.ratio[metric_index]??>
                ${(cell.ratio[metric_index] * 100)?string["0.0"]}%
            <#else>
                N/A
            </#if>
        </td>
    </#macro>
        <#-- preserved for reference, feel free to delete if no longer needed -->
        <div class="metric-section-wrapper hidden" rel="totalFlows">
            <table id='contributors-view-metric_0' class="contributors-table" cell-spacing="0" width="100%">

                <thead>
                </thead>
                <tbody>
                <!-- First time row-->
                <tr>
                    <td class=""></td>
                    <td class=""></td>
                    <td class=""></td>
                    <td colspan="3">00:00 PDT</td>
                    <td colspan="3">01:00 PDT</td>
                    <td colspan="3">02:00 PDT</td>
                    <td colspan="3">03:00 PDT</td>
                    <td colspan="3">04:00 PDT</td>
                    <td colspan="3">05:00 PDT</td>
                    <td colspan="3">06:00 PDT</td>
                    <td colspan="3">07:00 PDT</td>
                    <td colspan="3">08:00 PDT</td>
                </tr>
                <tr>
                    <td class="checkbox"></td>
                    <td class="dimenesion "></td>
                    <td class="dimesnion-value"></td>
                    <td>2,206,489</td>
                    <td>1,991,618</td>
                    <td>-5.3%</td>

                    <td>2,238,827</td>
                    <td>2,195,817</td>
                    <td class="highlight">-5.3%</td>

                    <td>2,328,033</td>
                    <td>2,396,889</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>
                </tr>
                <tr class="cumulative hidden">
                    <td></td>
                    <td class="dimesnion"></td>
                    <td class="dimesnion-value"></td>
                    <td>2,206,489</td>
                    <td>1,991,618</td>
                    <td>-5.3%</td>

                    <td>2,238,827</td>
                    <td>2,195,817</td>
                    <td class="highlight">-5.3%</td>

                    <td>2,328,033</td>
                    <td>2,396,889</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>

                    <td>2,502,234</td>
                    <td>2,579,584</td>
                    <td>-5.4%</td>
                </tr>

                <!-- Divider row-->
                <tr class="divider-row">
                    <!-- Metric name -->
                    <td>totalFlows</td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td></td>
                </tr>
                <!-- Second time row-->
                <tr>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td colspan="3">00:00 PDT</td>
                    <td colspan="3">01:00 PDT</td>
                    <td colspan="3">02:00 PDT</td>
                    <td colspan="3">03:00 PDT</td>
                    <td colspan="3">04:00 PDT</td>
                    <td colspan="3">05:00 PDT</td>
                    <td colspan="3">06:00 PDT</td>
                    <td colspan="3">07:00 PDT</td>
                    <td colspan="3">08:00 PDT</td>
                </tr>
                <!-- Dimension values -->
                <tr class="hourly-values">
                    <td><input value="1" type="checkbox"></td>
                    <td class="dimesnion">countryCode</td>
                    <td class="dimesnion-value">us</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>
                </tr>

                <tr class="hourly-values">
                    <td><input value="1" type="checkbox"></td>
                    <td class="dimesnion">countryCode</td>
                    <td class="dimesnion-value">in</td>

                    <td class="baseline-value">422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>
                </tr>

                <!-- Cumulated values -->
                <tr class="cumulative-values">
                    <td><input value="1" type="checkbox"></td>
                    <td class="dimesnion">countryCode</td>
                    <td class="dimesnion-value">us</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>
                </tr>

                <tr class="cumulative-values">
                    <td><input value="1" type="checkbox"></td>
                    <td class="dimesnion">countryCode</td>
                    <td class="dimesnion-value">in</td>

                    <td class="baseline-value">422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>

                    <td class="baseline-value" >422,022.48</td>
                    <td class="current-value">428,169.51</td>
                    <td class="delta-ratio">1.457</td>
                </tr>


                </tbody>
                <tfoot>
                <tr>
                    <th>Total:</th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                    <th></th>
                </tr>
                </tfoot>
            </table>
            </div>
    </div>
</div>
