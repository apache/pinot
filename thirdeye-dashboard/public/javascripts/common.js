function generateHeatMap(dimension, tuples, numColumns, selectCallback) {

    // Generate cells
    var cells = []
    for (var i = 0; i < tuples.length; i++) {
        if (tuples[i]["baseline"] > 0) {
            var ratio = (tuples[i]["current"] - tuples[i]["baseline"]) / (1.0 * tuples[i]["baseline"]);
            var alpha = tuples[i]["prob"];
            var link = $('<a href="#" dimension="' + dimension + '">' + tuples[i]['value'] + '</a>');
            var cell = $('<td></td>');
            link.attr("title", "(current=" + tuples[i]["current"] + ",baseline=" + tuples[i]["baseline"] + ")");
            cell.addClass('cell');
            cell.append(link);
            cell.append('</br>' + (ratio.toPrecision(3) * 100).toFixed(2) + '%');
            cell.css('background-color', 'rgba(136,138,252,' + alpha + ')');
            cells.push(cell);
            $(link).click(selectCallback);
        }
        // TODO: Make an option to show these cells, but for now, just ignore them
//        else {
//            // Grayed out cell
//            var cell = $('<td>' + tuples[i]['value'] + '<br/>N/A</td>');
//            cell.addClass('disabled-cell');
//            cells.push(cell);
//        }
    }


    // Create table
    var table = $("<table class='heatmap'></table>");
    table.append($("<caption>" + dimension + "</caption>"))
    var batch = [];
    for (var i = 0; i < cells.length; i++) {
        batch.push(cells[i]);
        if (batch.length == numColumns) {
            var row = $("<tr></tr>");
            for (var j = 0; j < batch.length; j++) {
                row.append(batch[j]);
            }
            table.append(row);
            batch = [];
        }
    }
    if (batch.length > 0) { // any stragglers
        var row = $("<tr></tr>");
        for (var i = 0; i < batch.length; i++) {
            row.append(batch[i]);
        }
        table.append(row);
    }

    return table;
}

function addLogValue(tuples, keyName) {
    var logKeyName = "log" + keyName.charAt(0).toUpperCase() + keyName.slice(1);
    for (var i = 0; i < tuples.length; i++) {
        tuples[i][logKeyName] = Math.log(tuples[i][keyName])
    }
}

function getStats(tuples, keyName) {
    sum = 0
    sumSquares = 0
    for (var i = 0; i < tuples.length; i++) {
        if (tuples[i][keyName] !== Number.NEGATIVE_INFINITY) {
            sum += tuples[i][keyName]
            sumSquares += tuples[i][keyName] * tuples[i][keyName]
        }
    }
    return [sum / tuples.length, (sumSquares - (sum * sum) / tuples.length) / (tuples.length - 1)]
}

function normalize(tuples, keyName, stats) {
    var mean = stats[0]
    var stdev = Math.sqrt(stats[1])
    for (var i = 0; i < tuples.length; i++) {
        var score = (tuples[i][keyName] - mean) / stdev
        tuples[i]['score'] = score
        tuples[i]['prob'] = normalcdf(score)
    }
}

// from: http://www.math.ucla.edu/~tom/distributions/normal.html
function normalcdf(X){   //HASTINGS.  MAX ERROR = .000001
    var T=1/(1+.2316419*Math.abs(X));
    var D=.3989423*Math.exp(-X*X/2);
    var Prob=D*T*(.3193815+T*(-.3565638+T*(1.781478+T*(-1.821256+T*1.330274))));
    if (X>0) {
        Prob=1-Prob
    }
    return Prob
}
