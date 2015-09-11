<script src="/assets/js/thirdeye.dimension.heatmap.js"></script>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>

<div class="collapser"><h2>(-) Dimension Heat Map</h2></div>
<div id="dimension-heat-map-area">

      <#list dimensionView.view.metricNames as metric>
      <div class="collapser"><h3>(-) ${metric} </h3></div>
      <div class="metric-wrapper">
          <div class="collapser"><h4>(-) ${metric} heat map</h4></div>
          <div id="dimension-heat-map-tree-map-section" class="treemap-section">
              <div class="dimension-heat-map-container-title">
                  <h2>${metric}</h2>
                  <table style="width:100%; table-layout:fixed;">
                        <tr>
                            <th> Baseline Date: </th>
                            <th> Current Date: </th>
                            <th> Baseline Total:</th>
                            <th> Current Total:</th>
                            <th> Delta Value:</th>
                            <th> Delta (%):</th>
                            </tr>
                        <tr>
                            <td class="title-stat baseline-date-time"></td>
                            <td class="title-stat current-date-time"></td>
                            <td class="title-stat baseline-total">${dimensionView.view.metricGlobalStats[metric]['baseline_total']}</td>
                            <td class="title-stat current-total">${dimensionView.view.metricGlobalStats[metric]['current_total']}</td>
                            <td class="title-stat delta-value">${dimensionView.view.metricGlobalStats[metric]['delta_absolute_change']}</td>
                            <td class="title-stat delta-ratio">${dimensionView.view.metricGlobalStats[metric]['delta_percent_change']}</td>
                        </tr>
                  </table>
              </div>
              <div class="uk-button-group dimension-treemap-toggle-buttons" data-uk-button-radio>
                    <button class="uk-button dimension-treemap-mode" id="treemap_contribution-total-change-percent" mode="0">
                        <i class="uk-icon-eye-slash"></i> Percentage Change
                    </button>
                    <button class="uk-button dimension-treemap-mode" id="treemap_contribution-total-percent" mode="1">
                        <i class="uk-icon-eye-slash"></i> Contribution Change (%)
                    </button>
                    <button class="uk-button dimension-treemap-mode" id="treemap_contribution-change-percent" mode="2">
                        <i class="uk-icon-eye-slash"></i> Contribution to overall Change (%)
                    </button>
              </div>

              <div id="metric_${metric?index}_treemap_0" class="treemap-container" style="width: 100%; height: auto; overflow: hidden;" mode="0">
                    <table class="treemap-display-tbl">

                    <#list dimensionView.view.dimensionNames as dimension>
                              <tr>
                                  <td class="treemap-display-tbl-dim"><div class="treemap-rotate"><div>${dimension}</div></div></td><td id="metric_${metric?index}_dim_${dimension?index}_treemap_0" class="dimension-treemap" rel="${dimension}" style="width: 100%; height:100px;display:inline-block" ></td>
                              </tr>
                    </#list>
                    </table>

                </div>
                <div id="metric_${metric?index}_treemap_1" class="treemap-container" style="width: 100%; height: auto; overflow: hidden;" mode="1">
                    <table class="treemap-display-tbl">

                    <#list dimensionView.view.dimensionNames as dimension>
                              <tr>
                                  <td class="treemap-display-tbl-dim"><div class="treemap-rotate"><div>${dimension}</div></div></td><td id="metric_${metric?index}_dim_${dimension?index}_treemap_1" class="dimension-treemap" rel="${dimension}" style="width: 100%; height:100px;display:inline-block" ></td>
                              </tr>
                    </#list>
                    </table>

                </div>
                <div id="metric_${metric?index}_treemap_2" class="treemap-container" style="width: 100%; height: auto; overflow: hidden;" mode="2">
                    <table class="treemap-display-tbl">

                    <#list dimensionView.view.dimensionNames as dimension>
                              <tr>
                                  <td class="treemap-display-tbl-dim"><div class="treemap-rotate"><div>${dimension}</div></div></td><td id="metric_${metric?index}_dim_${dimension?index}_treemap_2" class="dimension-treemap" rel="${dimension}" style="width: 100%; height:100px;display:inline-block" ></td>
                              </tr>
                    </#list>
                    </table>
              </div>
          </div>
          <div class="collapser"><h4>(-) ${metric} heat map tabular view</h4></div>
          <div class="dimension-heat-map-table-section">
                   <div id='div-tabularView-metric_${metric?index}' class="dimension-heat-map-container-title">
                     <h2>${metric}</h2>
                       <table style="width:100%; table-layout:fixed;">
                           <tr>
                               <th> Baseline Date: </th>
                               <th> Current Date: </th>
                               <th> Baseline Total:</th>
                               <th> Current Total:</th>
                               <th> Delta Value:</th>
                               <th> Delta (%):</th>
                           </tr>
                           <tr>
                               <td class="title-stat baseline-date-time"></td>
                               <td class="title-stat current-date-time"></td>
                               <td class="title-stat baseline-total">${dimensionView.view.metricGlobalStats[metric]['baseline_total']}</td>
                               <td class="title-stat current-total">${dimensionView.view.metricGlobalStats[metric]['current_total']}</td>
                               <td class="title-stat delta-value">${dimensionView.view.metricGlobalStats[metric]['delta_absolute_change']}</td>
                               <td class="title-stat delta-ratio">${dimensionView.view.metricGlobalStats[metric]['delta_percent_change']}</td>
                           </tr>
                       </table>
                   </div>
                    <div class="uk-button-group dimension-tabular-column-toggle-buttons" data-uk-button-checkbox>
                        <button class="uk-button toggle-vis" data-column="7" rel="contribution-total-change-percent">
                            <i class="uk-icon-eye-slash"></i> Contribution to Total Change (%)
                        </button>
                        <button class="uk-button toggle-vis" data-column="8" rel="contribution-total-percent">
                            <i class="uk-icon-eye-slash"></i> Contribution to Total (%)
                        </button>
                        <button class="uk-button toggle-vis" data-column="9" rel="contribution-change-percent">
                            <i class="uk-icon-eye-slash"></i> Contribution Change (%)
                        </button>
                    </div>

                   <table id='tabularView-metric_${metric?index}' class="display compact" cell-spacing="0" width="100%">
                        <thead>
                        <tr id="filter-header" class="filter-header">
                            <th class="filter-header-m" rel="dimension">Dimension</th>
                            <th id="rank" class="filter-header-s" rel="rank">Rank</th>
                            <th           class="filter-header-m" rel="dimension-value">Dimension Value</th>
                            <th           class="filter-header-s" rel="baseline">Baseline</th>
                            <th           class="filter-header-s" rel="current">Current</th>
                            <th           class="filter-header-s" rel="delta-val">Delta (Value)</th>
                            <th           class="filter-header-s" rel="delta-ratio">Delta (%)</th>
                            <th           class="filter-header-s" rel="contribution_to_total_change_val">Contribution to Total Change</th>
                            <th           class="filter-header-s" rel="contribution_to_total_change_ratio">Contribution to Total (%)</th>
                            <th           class="filter-header-s" rel="contribution_change_ratio">Contribution Change (%)</th>
                        </tr>
                        <tr>
                            <th>Dimension</th>
                            <th>Rank</th>
                            <th>Dimension Value</th>
                            <th>Baseline</th>
                            <th>Current</th>
                            <th>Delta (Value)</th>
                            <th>Delta (%)</th>
                            <th>Contribution to Total Change (%)</th>
                            <th>Contribution to Total (%)</th>
                            <th>Contribution Change (%)</th>
                        </tr>

                        </thead>

                        <tbody>
                            <#list dimensionView.view.heatMaps as heatMap>
                                <#list heatMap.cells as cell>
                                <#if (heatMap.metric == metric)>
                                <tr role="row" class="${cell?item_cycle('even','odd')}">
                                    <td>${heatMap.dimension}</td>
                                    <td>${cell?index}</td>
                                    <#if (cell.value?html == "")>
                                        <td dimension-val class="datatable-empty-cell"></td>
                                    <#else>
                                        <td dimension-val>${cell.value?html}</td>
                                    </#if>
                                    <td>${cell.statsMap['baseline_value']?string["0.##"]}</td>
                                    <td>${cell.statsMap['current_value']?string["0.##"]}</td>
                                    <td>${(cell.statsMap['current_value'] - cell.statsMap['baseline_value'])?string["0.##"]} </td>
                                    <#if (cell.statsMap['baseline_value'] > 0)>
                                      <td>${(cell.statsMap['current_value'] - cell.statsMap['baseline_value'])/(cell.statsMap['baseline_value']) * 100}</td>
                                    <#else>
                                      <td>0</td>
                                    </#if>
                                    <td> ${cell.statsMap['volume_difference'] * 100}</td>
                                    <td> ${cell.statsMap['current_ratio'] * 100}</td>
                                    <td> ${cell.statsMap['contribution_difference'] * 100}</td>
                                </tr>
                                </#if>
                                </#list>
                            </#list>
                      </tbody>
                      <tfoot>
                      <tr>
                            <th>Dimension</th>
                            <th>Rank</th>
                            <th>Dimension Value</th>
                            <th>Baseline</th>
                            <th>Current</th>
                            <th>Delta (Value)</th>
                            <th>Delta (%)</th>
                            <th>Contribution to Total Change (%)</th>
                            <th>Contribution to Total (%)</th>
                            <th>Contribution Change (%)</th>
                        </tr>
                      </tfoot>
                </table>
          </div>
      </div>
      </#list>
</div>

<!--<div class="dimension-heat-map-heat-map-section">

    <div class="uk-button-group heat-map-buttons" data-uk-button-radio>
        <button class="uk-button dimension-heat-map-mode" id="dimension-heat-map-mode-self" mode="self">Self</button>
        <button class="uk-button dimension-heat-map-mode" id="dimension-heat-map-mode-others" mode="others">Others</button>
        <button class="uk-button dimension-heat-map-mode" id="dimension-heat-map-mode-all" mode="all">All</button>
    </div>
    <div class="heat-map-buttons">
        <button id="dimension-heat-map-filter" class="uk-button dimension-heat-map-filter-btn data-uk-tooltip" title="Only show elements with 1% or greater volume change" state="on">
             <i id="heat-map-filter-icon" class="uk-icon-filter"></i>
        </button>
    </div>

    <div id="dimension-heat-map-explanation">
      <div id="dimension-heat-map-explanation-self">
        <p>
          Shows percent change with respect to self: <code>(current - baseline) / baseline</code>
        </p>
        <p>
          <em>
            This view is appropriate for analyzing dimension values in isolation
          </em>
        </p>
      </div>
      <div id="dimension-heat-map-explanation-others">
        <p>
          Shows baseline percentage of whole, and difference with respect to current ratio: <br/>
          <code>baseline / sum(baseline)</code> +/-<code>(current / sum(current) - baseline / sum(baseline))</code>
        </p>
        <p>
          <em>
            This view shows displacement among dimension values, which can be used to determine the change
            of a dimension's composition
          </em>
        </p>
      </div>
      <div id="dimension-heat-map-explanation-all">
        <p>
          Shows contribution to overall change: <code>(current - baseline) / sum(baseline)</code>
        </p>
        <p>
          <em>
            This view weights dimension values by the total, so it can be used to break down the change in a metric</br>
            (That is, the heat map cells sum to the overall change in the metric)
          </em>
        </p>
      </div>
      <p>
        Cells are ordered in descending order based on current value, and
        shaded based on baseline value (darker is greater)
      </p>
    </div>

    <div id="dimension-heat-map-section-heat-map-area">
        <#if (dimensionView.view.heatMaps?size == 0)>
            <div class="uk-alert uk-alert-warning">
                <p>No data available</p>
            </div>
        </#if>


        <div id="dimension-heat-map-container"></div>

        <div id="dimension-heat-map-data">
            <#list dimensionView.view.heatMaps as heatMap>
                <div class="dimension-view-heat-map"
                     id='dimension-view-heat-map-${heatMap.metric}-${heatMap.dimension?replace(".", "-")}'
                     metric='${heatMap.metric}'
                     metric-display='${heatMap.metricAlias!heatMap.metric}'
                     dimension='${heatMap.dimension}'
                     dimension-display='${heatMap.dimensionAlias!heatMap.dimension}'
                     stats-names='${heatMap.statsNamesJson}'>
                    <#list heatMap.cells as cell>
                        <div class='dimension-view-heat-map-cell'
                             value='${cell.value?html}'
                             stats='${cell.statsJson}'></div>
                    </#list>
                </div>
            </#list>
        </div>
    </div>

</div>-->


<script type="text/javascript">

    var Treemap = {

        drawChart : function() {
            var Tooltip = {

            <#global idx =0>
                tooltipData : google.visualization.arrayToDataTable([
                    ['id',  'metric','dimension','cellvalue','baseline_value', 'current_value','baseline_ratio', 'current_ratio','delta_percent_change', 'contribution_difference', 'volume_difference' ],
                <#list dimensionView.view.metricNames as metric>
                    <#list dimensionView.view.dimensionNames as dimension>
                        <#list dimensionView.view.heatMaps as heatMap>
                            <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                                <#list heatMap.cells as cell>
                                    <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                        [ '${idx}','${metric}', '${dimension}', '${cell.value?html}', ${cell.statsMap['baseline_value']?c}, ${cell.statsMap['current_value']?c}, ${cell.statsMap['baseline_ratio']?c},${cell.statsMap['current_ratio']?c} ,${cell.statsMap['delta_percent_change']?c}, ${cell.statsMap['contribution_difference']?c}, ${cell.statsMap['volume_difference']?c}],
                                        <#global  idx = idx + 1>
                                    </#if>
                                </#list>

                            </#if>
                        </#list>
                    </#list>
                </#list>
                ]),




                showTreeMapTooltip : function(row, size, value){


                    var currentTable = $(this.ma).attr('id')
                    var tableMetricDim = currentTable.substr(0, currentTable.length -10)
                    //console.log('tableMetricDim', tableMetricDim)
                    var dataMode = currentTable.substr(currentTable.length -1, 1)
                    var dataTable = tableMetricDim + '_data_' + dataMode
                    var indexStr =  Treemap[ dataTable ].getValue(row, 0)
                    if(isNaN(indexStr)){
                        return "";
                    }
                    var index = Number(indexStr)

                    //Tooltip Data columns
                    //['id',  'metric','dimension','cellvalue','baseline_value', 'current_value','baseline_ratio', 'current_ratio','delta_percent_change', 'contribution_difference', 'volume_difference' ],
                    var cellValue = Tooltip.tooltipData.getValue(Number(index), 3) == "" ? "unknown" : Tooltip.tooltipData.getValue(Number(index), 3)
                    var baseline = (Tooltip.tooltipData.getValue(Number(index), 4)).toFixed(2).replace(/\.?0+$/,'')
                    var current = (Tooltip.tooltipData.getValue(Number(index), 5)).toFixed(2).replace(/\.?0+$/,'')
                    var deltaValue = (current - baseline).toFixed(2).replace(/\.?0+$/,'')
                    var currentRatio = (Tooltip.tooltipData.getValue(Number(index), 7) * 100).toFixed(2).replace(/\.?0+$/,'');
                    var contributionDifference = (Tooltip.tooltipData.getValue(Number(index), 9) * 100).toFixed(2).replace(/\.?0+$/,'');
                    return '<div class="treemap-tooltip">' +
                            '<table>' +
                            '<tr><td>value : </td><td><a class="tooltip-link" href="#" rel="'+ Tooltip.tooltipData.getValue(Number(index), 2) +'">' +  cellValue + '</a></td></tr>' +
                            '<tr><td>baseline value : </td><td>' +  baseline + '</td></tr>' +
                            '<tr><td>current value : </td><td>'+ current + '</td></tr>' +
                            '<tr><td>delta value : </td><td>' + deltaValue + '</td></tr>' +
                            '<tr><td>delta (%) : </td><td>' + (Tooltip.tooltipData.getValue(Number(index), 8) *100).toFixed(2) + '</td></tr>' +
                            '<tr><td>change in contribution (%) : </td><td>' + currentRatio +'(' + contributionDifference +')' + '</td></tr>' +
                            '</table>' +
                            '</div>'
                }

            }
            var CreateNewQuery = {
                //Google Charts is catching the click event on the treemap cells and
                //provides the select event without the clickec cell display value and dimension value so we catch them on mousedown as
                //CreateNewQuery.dimension, CreateNewQuery.value
                mouseDownHandler : function(){
                  // TODO: click on the cell of the treemap should fix the value and dimension in the query and rerender the page
                },

                selectHandler : function(){
                 // TODO: click on the cell of the treemap should fix the value and dimension in the query and rerender the page

                }

            }

            var options = {
                maxDepth: 2,
                minColorValue: -25,
                maxColorValue: 25,
                minColor: '#f00',
                midColor: '#ddd',
                maxColor: '#0d0',
                headerHeight: 0,
                fontColor: 'black',
                showScale: false,
                highlightOnMouseOver: true,
                generateTooltip : Tooltip.showTreeMapTooltip
            }


        /**
         * Each cell in a data table has a Value(used as the id) and FormattedValue(what is shown).
         **/
        <#global idx_0 =0>
        <#global idx_1 =0>
        <#global idx_2 =0>
        <#list dimensionView.view.metricNames as metric>
            <#list dimensionView.view.dimensionNames as dimension>
            Treemap.metric_${metric?index}_dim_${dimension?index}_data_0 = google.visualization.arrayToDataTable([
                [{v:'uniqueID', f:'displayValue'},  'Parent', 'current ratio (size)', 'delta (color)'],
                [{v:'${metric}_${dimension}', f:'${dimension}'}, null, 0,  ${dimension?index}],
                <#list dimensionView.view.heatMaps as heatMap>
                    <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                        <#list heatMap.cells as cell>
                            <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                <#if (cell.value?has_content)>
                                    [{ v:'${.globals.idx_0}', f:'${cell.value} (${(cell.statsMap['delta_percent_change'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['delta_percent_change'] * 100)?c}],
                                <#else>
                                    [{v:'${.globals.idx_0}', f:'unknown (${(cell.statsMap['delta_percent_change'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['delta_percent_change'] * 100)?c}],
                                </#if>
                                <#global  idx_0 = idx_0 + 1>
                            </#if>
                        </#list> //end of heatmap Cell
                    </#if>
                </#list> //end of heatmap
            ]);

                Treemap.metric_${metric?index}_dim_${dimension?index}_data_1 = google.visualization.arrayToDataTable([
                    [{v:'uniqueID', f:'displayValue'},  'Parent', 'current ratio (size)', 'delta (color)'],
                    [{v:'${metric}_${dimension}', f:'${dimension}'}, null, 0,  ${dimension?index}],
                    <#list dimensionView.view.heatMaps as heatMap>
                        <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                            <#list heatMap.cells as cell>
                                <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                    <#if (cell.value?has_content)>
                                        [{v:'${.globals.idx_1}', f:'${cell.value} (${(cell.statsMap['contribution_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['contribution_difference'] * 100)?c}],
                                    <#else>
                                        [{v:'${.globals.idx_1}', f:'unknown (${(cell.statsMap['contribution_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['contribution_difference'] * 100)?c}],
                                    </#if>
                                    <#global  idx_1 = idx_1 + 1>
                                </#if>
                            </#list> //end of heatmap Cell
                        </#if>
                    </#list> //end of heatmap
                ]);
                Treemap.metric_${metric?index}_dim_${dimension?index}_data_2 = google.visualization.arrayToDataTable([
                    [{v:'uniqueID', f:'displayValue'},  'Parent', 'current ratio (size)', 'delta (color)'],
                    [{v:'${metric}_${dimension}', f:'${dimension}'}, null, 0,  ${dimension?index}],
                    <#list dimensionView.view.heatMaps as heatMap>
                        <#if (heatMap.metric == metric && heatMap.dimension == dimension)>
                            <#list heatMap.cells as cell>
                                <#if (cell.statsMap['current_ratio'] *100 > 1 )>
                                    <#if (cell.value?has_content)>
                                        [{v:'${.globals.idx_2}', f:'${cell.value} (${(cell.statsMap['volume_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['volume_difference'] * 100)?c}],
                                    <#else>
                                        [{v:'${.globals.idx_2}', f:'unknown (${(cell.statsMap['volume_difference'])?string.percent})'}, '${heatMap.metric}_${heatMap.dimension}', ${(cell.statsMap['current_value'])?c}, ${(cell.statsMap['volume_difference'] * 100)?c}],
                                    </#if>
                                    <#global idx_2 = idx_2 + 1>
                                </#if>
                            </#list> //end of heatmap Cell
                        </#if>
                    </#list> //end of heatmap
                ]);
                //treemap where 2 level depth is displayed
                var metric_${metric?index}_dim_${dimension?index}_treemap_0 = new google.visualization.TreeMap(document.getElementById('metric_${metric?index}_dim_${dimension?index}_treemap_0'));
                var metric_${metric?index}_dim_${dimension?index}_treemap_1 = new google.visualization.TreeMap(document.getElementById('metric_${metric?index}_dim_${dimension?index}_treemap_1'));
                var metric_${metric?index}_dim_${dimension?index}_treemap_2 = new google.visualization.TreeMap(document.getElementById('metric_${metric?index}_dim_${dimension?index}_treemap_2'));


                metric_${metric?index}_dim_${dimension?index}_treemap_0.draw(Treemap.metric_${metric?index}_dim_${dimension?index}_data_0, options);
                metric_${metric?index}_dim_${dimension?index}_treemap_1.draw(Treemap.metric_${metric?index}_dim_${dimension?index}_data_1, options);
                metric_${metric?index}_dim_${dimension?index}_treemap_2.draw(Treemap.metric_${metric?index}_dim_${dimension?index}_data_2, options);

                google.visualization.events.addListener( metric_${metric?index}_dim_${dimension?index}_treemap_0 , 'select', CreateNewQuery.selectHandler);
                google.visualization.events.addListener( metric_${metric?index}_dim_${dimension?index}_treemap_1 , 'select', CreateNewQuery.selectHandler);
                google.visualization.events.addListener( metric_${metric?index}_dim_${dimension?index}_treemap_2 , 'select', CreateNewQuery.selectHandler);

            </#list> //end of dimension
        </#list>  //end of metric



            //Preselect treeemap mode on pageload (mode 0 = Percentage Change)
            $(".dimension-treemap-mode[mode = '0']").click()

            $(".tooltip-link").click(function(e){

                //prevent the default jump to top behavior on <a href="#"></a>
                e.preventDefault()

                //get value and dimension from the current tooltip
                var value = ($(this).html().trim() == "unknown") ? "" : $(this).html().trim()

                var dimension = $(this).attr("rel")
                console.log('value', value)
                //fix the value and dimension in the query and redraw the page
                var dimensionValues = parseDimensionValues(window.location.search)
                dimensionValues[dimension] = value
                window.location.search = encodeDimensionValues(dimensionValues)
            })

            $(".treemap-container svg").on("mousedown", "g", CreateNewQuery.mouseDownHandler)

                //Preselect treeemap mode on pageload (mode 0 = Percentage Change)
               $(".dimension-treemap-mode[mode = '0']").click()

        }
    }



    google.load("visualization", "1", {packages:["treemap"]});
    google.setOnLoadCallback(Treemap.drawChart);





</script>