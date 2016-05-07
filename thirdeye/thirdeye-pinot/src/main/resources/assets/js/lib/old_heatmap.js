    //Treemap section
    function old_getHeatmap(){

        var url="/dashboard/data/heatmap?" + window.location.hash.substring(1);
        getData(url).done(function(data) {

            /* Handelbars template for treemap table */
            var result_treemap_template = HandleBarsTemplates.template_treemap(data)
            $("#"+ hash.view +"-display-chart-section").html(result_treemap_template);

            function drawChart(){

                var options = {
                    maxDepth: 2,
                    minColorValue: -25,
                    maxColorValue: 25,
                    minColor: '#f00',
                    midColor: '#ddd',
                    maxColor: '#00f',
                    headerHeight: 0,
                    fontColor: '#000',
                    showScale: false,
                    highlightOnMouseOver: true//,
                    //generateTooltip : showTreeMapTooltip
                }

                var numMetrics = data["metrics"].length
                for(var m =0; m< numMetrics;m++){
                	var metric = data["metrics"][m];
                	var len = data["dimensions"].length
                    for(var d= 0; d<len; d++){
                    	var dimension = data["dimensions"][d];
                        console.log("dimension:")
                        console.log(dimension)
                        var Treemap = {};

                        //Turn arrays into dataTables
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"] = new google.visualization.DataTable();


                        //Add cloumns
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"].addColumn('string','{ "v": "uniqueID", "f": "displayValue"}');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"].addColumn('string', 'Parent');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"].addColumn('number', 'current ratio (size)');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"].addColumn('number', 'delta (color)');

                        //Add first row
                        var parentRow =  metric +"_" + dimension ;
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"].addRow([ parentRow , null, 0, 0 ]);

                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"] = new google.visualization.DataTable();

                        //Add cloumns
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"].addColumn('string','{ "v": "uniqueID", "f": "displayValue"}');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"].addColumn('string', 'Parent');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"].addColumn('number', 'current ratio (size)');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"].addColumn('number', 'delta (color)');

                        //Add first row
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"].addRow([ parentRow , null, 0, 0 ]);
                        

                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"] = new google.visualization.DataTable();

                        //Add cloumns
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"].addColumn('string','{ "v": "uniqueID", "f": "displayValue"}');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"].addColumn('string', 'Parent');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"].addColumn('number', 'current ratio (size)');
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"].addColumn('number', 'delta (color)');

                        //Add first row
                        Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"].addRow([ parentRow , null, 0, 0 ]);


                        var dimensionData = data["data"][metric + "." + dimension]["responseData"]
                        console.log("dimensionData:")
                        console.log(dimensionData)
                        var schema = data["data"][metric + "." + dimension]["schema"]["columnsToIndexMapping"]
                        console.log("schema:")
                        console.log(schema)
                        var numDimValues = dimensionData.length;
                        for(valId = 0;valId<numDimValues;valId++){

                            var vVal = '' + m*d + valId;
                            var dimensionValue = dimensionData[valId][schema["dimensionValue"]]
                            if(dimensionValue == ""){
                                dimensionValue = "UNKNOWN";
                            };

                        	var fVal_0;
                        	if(dimensionData[valId][schema["percentageChange"]] >= 0){
                        		fVal_0 = dimensionValue + " ( +" + dimensionData[valId][schema["percentageChange"]] + "%)"
                        	} else{
                        		fVal_0 = dimensionValue + " ( " + dimensionData[valId][schema["percentageChange"]] + "%)"
                        	}
                        	var fVal_1;
                        	if(dimensionData[valId][schema["contributionDifference"]] >= 0) {
                        		fVal_1 = dimensionValue + " (" + dimensionData[valId][schema["baselineContribution"]] + " +" + dimensionData[valId][schema["contributionDifference"]] + "%)";
                        	} else {
                        		fVal_1 = dimensionValue + " (" + dimensionData[valId][schema["baselineContribution"]] + "  " + dimensionData[valId][schema["contributionDifference"]] + "%)";
                        	}
                        	var fVal_2;
                        	if(dimensionData[valId][schema["contributionToOverallChange"]] >= 0) {
                        		fVal_2 = dimensionValue + " ( +" + dimensionData[valId][schema["contributionToOverallChange"]] + "%)";
                        	} else{
                        		fVal_2 = dimensionValue + " (" + dimensionData[valId][schema["contributionToOverallChange"]] + "%)";
                        	}

                            var parent = metric + "_" + dimension;
                            var color_0 = parseFloat(dimensionData[valId][schema["deltaColor"]])

                            var size_0 = parseFloat(dimensionData[valId][schema["deltaSize"]])

                            var color_1 = parseFloat(dimensionData[valId][schema["contributionColor"]])
                            var size_1 = parseFloat(dimensionData[valId][schema["contributionSize"]])

                            var color_2 = parseFloat(dimensionData[valId][schema["contributionToOverallColor"]])
                            var size_2 = parseFloat(dimensionData[valId][schema["contributionToOverallSize"]])

                            fv = {'v': vVal , 'f' :fVal_0};
                        	Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"].addRow([fv, parent, size_0, color_0 ]);
                            fv = {'v': vVal , 'f' :fVal_1 };
                        	Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"].addRow([fv, parent, size_1, color_1]);
                            fv = {'v': vVal , 'f' :fVal_2};
                        	Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"].addRow([fv, parent, size_2, color_2]);
                        }

                        //Get the id of each placeholder on the page
                        Treemap["placeHolderID" + 0] = 'metric_' + metric + '_dim_' + d + '_treemap_0';
                        Treemap["placeHolderID" + 1] = 'metric_' + metric + '_dim_' + d + '_treemap_1';
                        Treemap["placeHolderID" + 2] = 'metric_' + metric + '_dim_' + d + '_treemap_2';


                        //pass the placeholders into the google chart treemap compiler
                        Treemap["metric_" + metric + "_dim_" + d + "_treemap_0"] = new google.visualization.TreeMap(document.getElementById(Treemap["placeHolderID" + 0]));
                        Treemap["metric_" + metric + "_dim_" + d + "_treemap_1"] = new google.visualization.TreeMap(document.getElementById(Treemap["placeHolderID" + 1]));
                        Treemap["metric_" + metric + "_dim_" + d + "_treemap_2"] = new google.visualization.TreeMap(document.getElementById(Treemap["placeHolderID" + 2]));


                        //pass the datatables and the options object into the google treemap compiler
                        Treemap["metric_" + metric + "_dim_" + d + "_treemap_0"].draw(Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_0"], options);
                        Treemap["metric_" + metric + "_dim_" + d + "_treemap_1"].draw(Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_1"], options);
                        Treemap["metric_" + metric + "_dim_" + d + "_treemap_2"].draw(Treemap["formattedData_metric_" + metric + "_dim_" + d + "_treemap_2"], options);

                        //Treemap["metric_"+ metric + "_tooltipData"] = google.visualization.arrayToDataTable(data[metric]["tooltip"])

                        //For tooltip:
                        //Treemap["dataTable"] = google.visualization.arrayToDataTable(data[metric]["tooltip"])
                    }

                    function showTreeMapTooltip(row, size, value){

                        var currentTable = $(this.ma).attr('id');

                        var tableMetricDim = currentTable.substr(0, currentTable.length -10);
                        var dataMode = currentTable.substr(currentTable.length -1, 1);
                        var dataTable = tableMetricDim; //+ '_data_' + dataMode
                        var indexStr =  Treemap[ dataTable ].getValue(row, 0);
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

                //After all the content loaded set the fontcolor of the cells based on the brightness of the background color
                function fontColorOverride(cell) {

                    var cellObj = $(cell);
                    var hex = cellObj.attr("fill");

                    var colorIsLight = function (r, g, b) {
                        // Counting the perceptive luminance
                        // human eye favors green color...
                        var a = 1 - (0.299 * r + 0.587 * g + 0.114 * b) / 255;
                        return (a < 0.5);
                    }


                    function hexToRgb(hex) {
                        // Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
                        var shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
                        hex = hex.replace(shorthandRegex, function (m, r, g, b) {
                            return r + r + g + g + b + b;
                        });

                        var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
                        return result ? {
                            r: parseInt(result[1], 16),
                            g: parseInt(result[2], 16),
                            b: parseInt(result[3], 16)
                        } : null;
                    }

                    if (hexToRgb(hex)) {
                        var textColor = colorIsLight(hexToRgb(hex).r, hexToRgb(hex).g, hexToRgb(hex).b) ? '#000000' : '#ffffff';
                        cellObj.next('text').attr('fill', textColor);
                    }

                };
                var area = $("#"+ hash.view +"-display-chart-section");
                $("rect", area).each( function(index,cell){
                    fontColorOverride(cell);
                });

                $("g", area).on("mouseout", function(event){
                    if($(event.currentTarget).prop("tagName") === "g"){
                        fontColorOverride($("rect", event.currentTarget));
                    }
                });
            }
            drawChart();

            //Treemap eventlisteners

            $(".dimension-treemap-mode").click(function() {

                var currentMode = $(this).attr('mode');
                var currentMetricArea = $(this).closest(".dimension-heat-map-treemap-section");

                // Display related treemap
                $(".treemap-container", currentMetricArea ).hide();
                $($(".treemap-container", currentMetricArea )[currentMode]).show();

                //Change icon on the radio buttons
                $('.dimension-treemap-mode i', currentMetricArea ).removeClass("uk-icon-eye");
                $('.dimension-treemap-mode i', currentMetricArea ).addClass("uk-icon-eye-slash");
                $('i', this).removeClass("uk-icon-eye-slash");
                $('i', this).addClass("uk-icon-eye");
            });

            //Set initial view

            //Preselect treeemap mode on pageload (mode 0 = Percentage Change)
            $(".dimension-treemap-mode[mode = '0']").click()

            //Indicate baseline total value increase/decrease with red/blue colors next to the title of the table and the treemap
            $(".title-box .delta-ratio, .title-box .delta-value").each(function(index, currentDelta){

                var delta = $(currentDelta).html().trim().replace(/[\$,]/g, '') * 1

                if ( delta != 0 && !isNaN(delta)){
                    var color = delta > 0 ? "blue plus-symbol" : "red"

                    $(currentDelta).addClass(color)
                }
            })

        });

        //Clicking a hetamap cell should fix the value in the filter
//        $("#main-view").on("click", "rect", function(){
//
//            var label = $(this).next().html();
//            var  dimensionValue = label.split(" (")[0];
//            var dimension = $(this).closest(".dimension-treemap").prevAll("td.treemap-display-tbl-dim").text();
//            var filters = readFiltersAppliedInCurrentView("compare");
//            filters[dimension] = [dimensionValue];
//
//            updateFilterSelection(filters);
//            hash.filters = encodeURIComponent(JSON.stringify(filters));
//            hash.view = "compare";
//            hash.aggTimeGranularity = "aggregateAll";
//            //update hash will trigger window.onhashchange event:
//            //update the form area and trigger the ajax call
//         window.location.hash = encodeHashParameters(hash);
//        })



    };
