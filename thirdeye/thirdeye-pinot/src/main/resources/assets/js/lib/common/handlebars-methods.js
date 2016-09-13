$(document).ready(function () {
    /** --- 1) Register Handelbars helpers --- * */

        //takes a string returns a HEX color code
    Handlebars.registerHelper('colorById', function (id, numIds, options) {

        if (typeof numIds == "object") {
            var keysAry = Object.keys(numIds);
            numIds = keysAry.length;
        }

        if (parseInt(numIds) < 10) {

            return d3.scale.category10().range()[id];

        } else if (parseInt(numIds) < 20) {
            return d3.scale.category20().range()[id];

        } else {
            return  Handlebars.helpers.assignColorByID(numIds, id)
        }
    });

    Handlebars.registerHelper('colorByIdContributors', function (id, dimensionValuesMap, options) {

        var numIds = dimensionValuesMap[options.hash.dimName].length

        if (parseInt(numIds) < 10) {
            return d3.scale.category10().range()[id];

        } else if (parseInt(numIds) < 20) {
            return d3.scale.category20().range()[id];

        } else {
            return  Handlebars.helpers.assignColorByID(numIds, id)
        }
    });


    //If you change this method change the colorScale() and assignColorByID()functions too
    //those 2 are defining the color of the lines on every timeseries chart when the number of items are > 20
    Handlebars.registerHelper('assignColorByID', function (len, index) {

        //16777216 = 256 ^ 3
        var diff = parseInt(16777216 / len);

        var diffAry = [];
        for (x = 0; x < len; x++) {
            diffAry.push(diff * x)
        }

        var colorAry = [];
        var num;
        for (y = 0; y < len; y++) {

            if (y % 2 == 0) {
                num = diffAry[y / 2]
            } else {
                num = diffAry[Math.floor(len - y / 2)]
            }


            var str = (num.toString(16) + "dddddd")
            var hex = num.toString(16).length < 6 ? "#" + str.substr(0, 6) : "#" + num.toString(16)
            colorAry.push(hex)
        }

        return colorAry[index]
    });

    //Assign hidden class to element if the 2 params are equal
    Handlebars.registerHelper('hide_if_eq', function (param1, param2) {
        if (param1 == param2) {
            return "uk-hidden"
        }
    });

    //Takes 2 parameters and a className variable, className to element if the 2 params are equal
    Handlebars.registerHelper('add_string_if_eq', function (param1, param2, options) {
        var stringToAdd = options.hash.stringToAdd;
        if (param1 == param2) {
            return stringToAdd
        }
    });

    Handlebars.registerHelper('displayAnomalyResultExploreDimensions', function (dimensionMap) {
        var dimensionString = "";
        var separator = "";
        var mapSize = 0;
        for (var dimensionName in dimensionMap) {
            if (dimensionMap.hasOwnProperty(dimensionName)) {
                dimensionString += separator + dimensionName + ":" + dimensionMap[dimensionName];
                separator = "; ";
                ++mapSize;
            }
        }
        if (mapSize == 0) {
            return "ALL";
        } else {
            return dimensionString;
        }
    });


    //Helper for anomaly function form, here we can set the desired display of any function property
    Handlebars.registerHelper('displayAnomalyFunctionProp', function (param , value) {

        switch(param){
            case "changeThreshold":
                value = Math.abs(parseFloat(value)) * 100;
            break;
            case "minConsecutiveSize":
                //This condition can be removed when all week over week type functions will have minConsecutiveSize property
                //when adding this property 09/2016,the earlier functions don't have the property
                if(!value){
                    value = 1;
                };
            break;
            default:
            break;
        }
        return value
    });

    //returns classname negative or positive or no classname. The classname related css creates a :before pseudo element triangle up or down
    Handlebars.registerHelper('describeDelta', function (value, describeMode) {

        var describeChange;
        if (parseFloat(value) > 0) {
            describeChange = {
                iconClass: 'positive-icon',
                description: 'INCREASES'}
        } else if (parseFloat(value) < 0) {
            describeChange = {
                iconClass: 'negative-icon',
                description: 'DROPS'}
        } else {
            return
        }

        return describeChange[describeMode]

    });

    //takes a value and if alias is available displays the alias
    //in dashboards the derived metrics can have an alias int he configuration
    Handlebars.registerHelper('displayAlias', function (name, alias) {

        if (alias != undefined && alias.length > 0) {
            return alias
        } else {
            return name
        }
    });

    //helper for tabular and contributor view only to display the ratio values in every 3rd cell
    Handlebars.registerHelper('displayRatio', function (val, index) {
        if ((index + 1) % 3 == 0 && parseFloat(val)) {
            return  val + "%";
        } else {
            return val.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        }
    });

    // If dimension value is null or "?" replace it with unknown or other
    Handlebars.registerHelper('displayDimensionValue', function (dimensionValue) {

        if (dimensionValue == "") {
            return "UNKNOWN";
        } else if (dimensionValue == "?") {
            return "OTHER";
        } else {
            return dimensionValue;
        }
    });


    //returns the dataAggregateGranularity of the dataset
    Handlebars.registerHelper('returnAggregateGranularity', function () {
         var aggTimeGranularity;
         if(window.datasetConfig && window.datasetConfig.hasOwnProperty("dataGranularity")) {
             aggTimeGranularity =  window.datasetConfig.dataGranularity;
        }else{
             aggTimeGranularity =  "HOURS";
         }
        return aggTimeGranularity;
    });

    //returns in users timezone
    Handlebars.registerHelper('returnUserTimeZone', function () {
        var tz = getTimeZone();
        return moment().tz(tz).format('z');
    });

    //takes utc date iso format ie. 2016-04-06T07:00:00.000Z, returns date and time in users timezone
    Handlebars.registerHelper('displayDate', function (date) {
        var tz = getTimeZone();
        return moment(date).tz(tz).format('YYYY-MM-DD h a z');
    });

    //takes utc timestamp (milliseconds ie. 1462626000000), returns date and time in users timezone
    //the options contain showTimeZone boolean param
    Handlebars.registerHelper('millisToDate', function (millis, options) {

        if (!millis) {
            return "n.a"
        }

        //Options
        var showTimeZone = options.hash.hasOwnProperty("showTimeZone") ? (options.hash.showTimeZone == false ? false : true) : true;
        var showYear = options.hash.hasOwnProperty("showYear") ? (options.hash.showYear == false ? false : true) : true;
        var onlyHour = options.hash.hasOwnProperty("onlyHour") ? (options.hash.onlyHour == false ? false : true) : false;
        var slashSeparator = options.hash.hasOwnProperty("slashSeparator") ? options.hash.slashSeparator : false;



        var displayDateFormat;
        if(onlyHour){
            displayDateFormat = 'h a'
        }else{

            if(showTimeZone){
                if (showYear){
                    displayDateFormat = (slashSeparator) ? 'YYYY/MM-DD h a z': 'YYYY-MM-DD h a z';
                }else{
                    displayDateFormat = (slashSeparator) ? 'MM/DD h a z' : 'MM-DD h a z';
                }
            }else{
               if(showYear) {
                   displayDateFormat = (slashSeparator) ? 'YYYY/MM/DD h a': 'YYYY-MM-DD h a';
               }else{
                   displayDateFormat = (slashSeparator) ? 'MM/DD h a': 'MM-DD h a';
               }
            }
        };

        millis = parseInt(millis);
        var tz = getTimeZone();
        return moment(millis).tz(tz).format(displayDateFormat);
    });

    //Takes 2 timestamps and returns a date range where the end date is hours only, month & day & hours, or full date based on the equality of the 2 dates
    Handlebars.registerHelper('displayDateRange', function (start, end) {

        if (!start || !end) {
            return "n.a"
        }

        //Options
        // var showTimeZone = options.hash.hasOwnProperty("showTimeZone") ? (options.hash.showTimeZone == false ? false : true) : true;

        var tz = getTimeZone();
        var startMillis = parseInt(start);
        var endMillis = parseInt(end);
        var startDate = moment(startMillis).tz(tz).format('YYYY/MM/DD');
        var endDate = moment(endMillis).tz(tz).format('YYYY/MM/DD');
        var startMonth = moment(startMillis).tz(tz).format('YYYY/MM');
        var endMonth = moment(endMillis).tz(tz).format('YYYY/MM');
        var startYear = moment(startMillis).tz(tz).format('YYYY');
        var endYear = moment(endMillis).tz(tz).format('YYYY');

        var startDateFormat = 'MM/DD h:m a';
        var endDateFormat;
        if (startYear == endYear) {
            if (startMonth == endMonth) {
                if (startDate == endDate) {
                    endDateFormat = 'h:m a'
                } else {
                    endDateFormat = 'MM/DD h:m a'
                }
            } else {
                endDateFormat = 'MM/DD h:m a'
            }
        } else {
            startDateFormat = 'YY/MM/DD h:m a'
            endDateFormat = 'YY/MM/DD h:m a'
        }

        return moment(startMillis).tz(tz).format(startDateFormat) + " - " + moment(endMillis).tz(
                tz).format(endDateFormat)
    });

    //takes utc timestamp (milliseconds ie. 1462626000000), returns date and time in users tz in a format in sync with the hash aggregate granularity
    Handlebars.registerHelper('millisToDateTimeInAggregate', function (millis) {

        if (!millis) {
            return "n.a"
        }
        millis = parseInt(millis);
        var tz = getTimeZone();
        var dateTimeFormat = "h a";
        if (hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS") {

            dateTimeFormat = "MM-DD h a"
        }

        return moment(millis).tz(tz).format(dateTimeFormat);
    });

    //Parse string that contains ':' and uses ',' separator
    Handlebars.registerHelper('parseProperties', function (str, prop) {

        if(str && str.substr(str.length - 1) == ","){
            str = str.substring(0, str.length-1);
        }
        str = str.replace(/,/g, ";")
        str = str.replace(/ /g, "")
        str = str.replace(/:/g, "=")
        var fnProperties = {}
        var propertiesAry = str.split(";");
        for (var i = 0, numProp = propertiesAry.length; i < numProp; i++) {
            var keyValue = propertiesAry[i];
            keyValue = keyValue.split("=")
            var key = keyValue[0];
            var value = keyValue[1];
            fnProperties[key] = value;
        }

        var val = fnProperties[prop];
        if(val){
            if(prop == "baseLineVal" ||  prop == "currentVal" ){
                val =  val.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
            }
            return val;
        }
        return

    });

    /* Add details-cell or heatmap-cell class to cells */
    Handlebars.registerHelper('classify', function (index) {
        if ((index + 1) % 3 == 0) {
            className = "heat-map-cell"
        } else if ((index + 1) % 3 == 1) {
            className = "details-cell border-left hidden"
        } else {
            className = "details-cell hidden"
        }
        return  className
    });

    //to get dimensionValue data for timebuckets ary in contributors ajax response
    Handlebars.registerHelper('returnValue', function (obj, options) {
        var responseData = obj.responseData;
        var schema = obj.schema.columnsToIndexMapping;
        var rowId = options.hash.key;
        var schemaItem = options.hash.schemaItem;
        var indexFordisplayRatioHelper = 0;
        if (schemaItem == "percentageChange") {
            indexFordisplayRatioHelper = 2;
        }
        return Handlebars.helpers.displayRatio(responseData[rowId][schema[schemaItem]], indexFordisplayRatioHelper)
    })

    //takes an object and a key as option param and returns an object as a scope
    Handlebars.registerHelper('lookupDimValues', function (obj, options) {
        //Without the options.fn()  the raw object would be returned to be the html content
        return options.fn(obj[options.hash.dimName])
    });

    //takes an object and a key and returns the value in ()-s
    Handlebars.registerHelper('lookupInMapByKey', function (mapObj, options) {
        var key = options.hash.key;
        var val = mapObj[key];
        if (typeof val !== "undefined") {

            if(options.hash.display=="()"){
                val = "(" + val + ")";
            }
        }
        return val;
    });

    //takes an object and 2 strings that will create one key to look up, returns object[key] as a scope ie. summary["metricName|dimensionName"]
    Handlebars.registerHelper('lookupComplexScope', function (obj, options) {
        //Without the options.fn()  the raw object would be returned to be the html content
        return options.fn(obj[options.hash.primary + options.hash.separator + options.hash.secondary ])
    });

    //takes an object and a key as option param and returns an object as a scope
    Handlebars.registerHelper('lookupRowIdList', function (obj, options) {
        //Without the options.fn()  the raw object would be returned to be the html content
        return options.fn(obj[options.hash.metricName + '|' + options.hash.dimName + '|' + options.hash.dimValue ])
    });


    //if param 1 == param 2
    Handlebars.registerHelper('if_eq', function (a, b, options) {
        if (a == b) { // Or === depending on your needs
            return options.hash.value
        }

    });

    HandleBarsTemplates = {

    }
    /** --- 2) Create Handelbars templating method --- * */

    var source_tab_template = $("#tab-template").html();
    HandleBarsTemplates.template_tab = Handlebars.compile(source_tab_template);

    var source_form_template = $("#form-template").html();
    HandleBarsTemplates.template_form = Handlebars.compile(source_form_template);

    var source_datasets_template = $("#datasets-template").html();
    HandleBarsTemplates.template_datasets = Handlebars.compile(source_datasets_template);

    var source_metric_list_template = $("#metric-list-template").html();
    HandleBarsTemplates.template_metric_list = Handlebars.compile(source_metric_list_template);

    var source_filter_dimension_value_template = $("#filter-dimension-value-template").html();
    HandleBarsTemplates.template_filter_dimension_value = Handlebars.compile(source_filter_dimension_value_template);

    var source_funnels_table = $("#funnels-table-template").html();
    HandleBarsTemplates.template_funnels_table = Handlebars.compile(source_funnels_table);

    var source_contributors_table = $("#contributors-table-template").html();
    HandleBarsTemplates.template_contributors_table = Handlebars.compile(source_contributors_table);

    var source_treemap_template = $("#treemap-template").html();
    HandleBarsTemplates.template_treemap = Handlebars.compile(source_treemap_template);

    var source_treemap_summary_template = $("#heatmap-summary-template").html();
    HandleBarsTemplates.template_treemap_summary = Handlebars.compile(source_treemap_summary_template);

    var source_metric_time_series_section = $("#metric-time-series-section-template").html();
    HandleBarsTemplates.template_metric_time_series_section = Handlebars.compile(source_metric_time_series_section);

    var source_time_series_template = $("#time-series-template").html();
    HandleBarsTemplates.template_time_series = Handlebars.compile(source_time_series_template);

    var source_anomaly_details_template = $("#anomaly-details-template").html();
    HandleBarsTemplates.template_anomaly_details = Handlebars.compile(source_anomaly_details_template);

    var source_anomalies_template = $("#anomalies-template").html();
    HandleBarsTemplates.template_anomalies = Handlebars.compile(source_anomalies_template);

    var source_anomaly_function_form_template = $("#anomaly-function-form-template").html();
    HandleBarsTemplates.template_anomaly_function_form = Handlebars.compile(source_anomaly_function_form_template);

    var source_existing_anomaly_functions_template = $("#self-service-existing-anomaly-functions-template").html();
    HandleBarsTemplates.template_existing_anomaly_functions = Handlebars.compile(source_existing_anomaly_functions_template);

    var source_self_service_email_template = $("#self-service-email-template").html();
    HandleBarsTemplates.template_self_service_email = Handlebars.compile(source_self_service_email_template);
})
