function getHeatmap() {

    //Todo: add the real endpoint
    var url = "/dashboard/data/heatmap?" + window.location.hash.substring(1);
    getData(url).done(function(data) {


        renderD3heatmap(data);

        heatMapEventListeners()

    });
};

function renderD3heatmap(data) {

    /* Handelbars template for treemap table */
    var result_treemap_template = HandleBarsTemplates.template_treemap(data)
    $("#"+ hash.view +"-display-chart-section").html(result_treemap_template);

    var numMetrics = data["metrics"].length
    for(var m =0; m< numMetrics;m++) {
        var metric = data["metrics"][m];

        var numDimensions = data["dimensions"].length
        for (var d = 0; d < numDimensions; d++) {
            var dimension = data["dimensions"][d];


            var dimensionData = data["data"][metric + "." + dimension]["responseData"]
            var schema = data["data"][metric + "." + dimension]["schema"]["columnsToIndexMapping"]


            //PARSE DATA
            var root_0 = {}
            var root_1 = {}
            var root_2 = {}
            root_0.name = dimension;
            root_1.name = dimension;
            root_2.name = dimension;

            var children_0 = [];
            var children_1 = [];
            var children_2 = [];
            var numDimValues = dimensionData.length;
            for(valId = 0;valId<numDimValues;valId++){

                var dimensionValue = dimensionData[valId][schema["dimensionValue"]]
                //Todo: remove this "" handler once backend is adding it to other
                if(dimensionValue == ""){
                    dimensionValue = "UNKNOWN";
                };

                var color_0 =  parseFloat(dimensionData[valId][schema["deltaColor"]]); //percentageChange
                var color_1 =  parseFloat(dimensionData[valId][schema["contributionColor"]]); //baselineContribution
                var color_2 =  parseFloat(dimensionData[valId][schema["contributionToOverallColor"]]); // contributionToOverallChange

                var delta_0 =  parseFloat(dimensionData[valId][schema["percentageChange"]]);
                var delta_1 =  parseFloat(dimensionData[valId][schema["baselineContribution"]]);
                var delta_2 =  parseFloat(dimensionData[valId][schema["contributionToOverallChange"]]);

                var opacity_0 = parseFloat("0." + Math.abs(Math.round(color_0)));
                var opacity_1 = parseFloat("0." + Math.abs(Math.round(color_1)));
                var opacity_2 = parseFloat("0." + Math.abs(Math.round(color_2)));

                var fontColor_0 = opacity_0 < 0.3 ? '#000000' : '#ffffff';
                var fontColor_1 = opacity_1 < 0.3 ? '#000000' : '#ffffff';
                var fontColor_2 = opacity_2 < 0.3 ? '#000000' : '#ffffff';

                var backgroundColor_0 =  Math.round(color_0) < 0 ? "rgba(255,0,0, " + opacity_0 + ")" :  ( Math.round(color_0) > 0 ? "rgba(0,0,255," + opacity_0 + ")" : "rgba(221,221,221,1)");
                var backgroundColor_1 =  Math.round(color_1) < 0 ? "rgba(255,0,0, " + opacity_1 + ")" :  ( Math.round(color_1) > 0 ? "rgba(0,0,255," + opacity_1 + ")" : "rgba(221,221,221,1)");
                var backgroundColor_2 =  Math.round(color_2) < 0 ? "rgba(255,0,0, " + opacity_2 + ")" :  ( Math.round(color_2) > 0 ? "rgba(0,0,255," + opacity_2 + ")" : "rgba(221,221,221,1)");

                var label_0 = delta_0 <= 0 ? dimensionValue +" (" + delta_0 + "%)" : dimensionValue +" (+" + delta_0 + "%)";
                var label_1 = delta_1 <= 0 ? dimensionValue +" (" + delta_1 + "%)" : dimensionValue +" (+" + delta_1 + "%)";
                var label_2 = delta_2 <= 0 ? dimensionValue +" (" + delta_2 + "%)" : dimensionValue +" (+" + delta_2 + "%)";


                var size = parseFloat(dimensionData[valId][schema["deltaSize"]]);

                //Todo: add proper tooltip
                var tooltip = "value: " + dimensionValue+ ",                                                                       "+
                    "baseline value: " + dimensionData[valId][schema["baselineValue"]] + ",                                                                "+
                    "current value: " +  dimensionData[valId][schema["currentValue"]] + ",                                                "+
                    "delta: " + dimensionData[valId][schema["percentageChange"]] + "%" + ",                                                  "+
                    "change in contribution (%): " +  dimensionData[valId][schema["contributionDifference"]];


//                var tooltip = "<table><tbody><tr><td>value : </td><td>" + dimensionValue+ "</td></tr>" +
//                    "<tr><td>baseline value : </td><td>" + dimensionData[valId][schema["baselineValue"]] + "</td></tr>" +
//                    "<tr><td>current value : </td><td>" + dimensionData[valId][schema["currentValue"]] + "</td></tr>" +
//                    "<tr><td>delta value : </td><td></td></tr>" +
//                    "<tr><td>delta: </td><td>" + dimensionData[valId][schema["percentageChange"]] + "%" + "</td></tr>" +
//                    "<tr><td>change in contribution (%) : </td><td>" + dimensionData[valId][schema["contributionDifference"]] + "</td></tr>" +
//                    "</tbody></table>"
                children_0.push({ "name": dimensionValue  ,"size": size, "label": label_0, "bgcolor": backgroundColor_0, "color": fontColor_0, "tooltip": tooltip});
                children_1.push({ "name": dimensionValue ,"size": size, "label": label_1, "bgcolor": backgroundColor_1, "color": fontColor_1, "tooltip": tooltip});
                children_2.push({ "name": dimensionValue ,"size": size, "label": label_2, "bgcolor": backgroundColor_2, "color": fontColor_2, "tooltip": tooltip});

            }
            root_0.children = children_0;
            root_1.children = children_1;
            root_2.children = children_2;

            //CHART LAYOUT
            var margin = {top: 0, right: 0, bottom: 5, left: 0};
            var width = 0.65 * window.innerWidth;
            var height = window.innerHeight * 0.7 / numDimensions;


            var placeholder_0 = '#metric_' + metric + '_dim_' + d + '_treemap_0'
            var placeholder_1 = '#metric_' + metric + '_dim_' + d + '_treemap_1'
            var placeholder_2 = '#metric_' + metric + '_dim_' + d + '_treemap_2'

            //DRAW TREEMAP
            function drawTreemap(root, placeholder){
                var treemap = d3.layout.treemap()
                    .size([width, height])
                    .sticky(true)
                    .sort(function(a,b) {
                       return a.value - b.value;
                    })
                    .value(function (d) {
                        return d.size;
                    });

                var div = d3.select(placeholder).append("div")//placeholder:
                    .style("position", "relative")
                    .style("width", width + "px")
                    .style("height", (height + margin.top + margin.bottom) + "px")
                    .style("left", margin.left + "px")
                    .style("top", margin.top + "px");

                var node = div.datum(root).selectAll(".node")
                    .data(treemap.nodes)
                    .enter().append("div")
                    .attr("class", "node")
                    .attr("data-dimension", function (d) {
                        return root.name
                    })
                    .attr("id", function (d) {return d.name})
                    .attr("data-uk-tooltip", true)
                    .attr("title", function (d) {return d.tooltip})
                    .call(position)
                    .style("background", function (d) {return d.children ? null : d.bgcolor})
                    .style("text-align", "center")
                    .style("color", function (d) {return d.color})
                    .style("font-size", "12px")
                    .text(function (d) {
                        return d.children ? null : d.label
                    });

                function position() {
                    this.style("left", function (d) {
                        return d.x + "px";
                    })
                        .style("top", function (d) {
                            return d.y + "px";
                        })
                        .style("width", function (d) {
                            return Math.max(0, d.dx - 1) + "px";
                        })
                        .style("height", function (d) {
                            return Math.max(0, d.dy - 1) + "px";
                        });
                }
            }

            drawTreemap(root_0, placeholder_0 )
            drawTreemap(root_1, placeholder_1 )
            drawTreemap(root_2, placeholder_2 )

        }
    }
}

function heatMapEventListeners(){
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

    //Clicking a hetamap cell should fix the value in the filter
        $("#"+ hash.view +"-display-chart-section").on("click", "div.node", function(){

            var  dimensionValue = $(this).attr("id");
            if(dimensionValue.toLowerCase() == "other" ||  dimensionValue.toLowerCase() == "unknown") {
                alert("'Other' or 'unknown' value cannot be the filter.");
            }else{

                var dimension = $(this).attr("data-dimension")
                var filters = readFiltersAppliedInCurrentView(hash.view);
                filters[dimension] = [dimensionValue];

                updateFilterSelection(filters);

                hash.filters = encodeURIComponent(JSON.stringify(filters));
                hash.aggTimeGranularity = "aggregateAll";

                //update hash will trigger window.onhashchange event:
                //update the form area and trigger the ajax call
                window.location.hash = encodeHashParameters(hash);
            }
        })

}
