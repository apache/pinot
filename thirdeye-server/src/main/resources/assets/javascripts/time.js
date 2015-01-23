/** Add time spinner to prototype */
$.widget("ui.timespinner", $.ui.spinner, {
    options: {
        step: 60 * 1000,
        page: 60
    },

    _parse: function(value) {
        if (typeof value === 'string') {
            if (Number(value) == value) {
                return Number(value);
            }
            return +Globalize.parseDate(value);
        }
        return value;
    },

    _format: function(value) {
        return Globalize.format(new Date(value), "t");
    }
});

/**
 * Uses date input and baseline selection to update the baseline display date
 */
function loadBaseline() {
    var date = $("#date-picker").val()
    var baseline = $("#baseline").val()
    var baselineMillis = baseline * 7 * 24 * 3600 * 1000
    var dateMillis = Date.parse(date)
    var baselineDate = new Date(dateMillis - baselineMillis)
    $("#baseline-display").html(baselineDate.toDateString())
}

/**
 * Ensures that the query time is within the loaded data set
 */
function checkTime(collectionTime) {
    var minTime = parseInt($("#min-time"))
    var maxTime = parseInt($("#max-time"))
    if (collectionTime < minTime || collectionTime > maxTime) {
        var message = "Out of range " + tickFormatter(minTime) + " to " + tickFormatter(maxTime);
        alert(message);
        throw message;
    }
}

/**
 * Formats a collection time as a UTC string
 */
function tickFormatter(collectionTime) {
    var date = new Date(collectionTimeToMillis(collectionTime));
    return date.toUTCString();
}

/**
 * Computes the baseline date given current date and delta
 */
function getBaselineDate(currentDate, deltaWeeks) {
    date = new Date(currentDate.getTime());
    date.setDate(date.getDate() - deltaWeeks * 7);
    return date;
}

/**
 * Extracts the date / time inputs and creates a Date object
 */
function getCurrentDate() {
    date = $("#date-picker").val();
    time = $("#spinner").val();
    millis = Date.parse(date + " " + time + " GMT");
    return new Date(millis)
}

/**
 * Extracts the timeWindow from slider
 */
function getTimeWindowMillis() {
    return collectionTimeToMillis($("#time-window").val());
}

/**
 * Converts milliseconds to the collection time
 */
function millisToCollectionTime(millis) {
    return Math.floor(millis / getFactor() / parseInt($("#time-window-size").html()));
}

/**
 * Converts collection time to milliseconds
 */
function collectionTimeToMillis(collectionTime) {
    return collectionTime * getFactor() * parseInt($("#time-window-size").html());
}

function getFactor() {
    var unit = $("#time-window-unit").html();
    if (unit == 'SECONDS') {
        return 1000;
    } else if (unit == 'MINUTES') {
        return 60 * 1000;
    } else if (unit == 'HOURS') {
        return 60 * 60 * 1000;
    } else if (unit == 'DAYS') {
        return 24 * 60 * 60 * 1000;
    }
    return 1;
}

function formatDatePickerInput(date) {
    month = date.getUTCMonth() + 1;
    day = date.getUTCDate();
    year = date.getUTCFullYear();
    return month + '/' + day + '/' + year;
}

function formatSpinnerInput(date) {
    hours = date.getUTCHours();
    minutes = date.getUTCMinutes();
    ampm = hours >= 12 ? 'pm' : 'am';
    hours %= 12;
    hours = hours ? hours : 12;
    minutes = minutes < 10 ? '0' + minutes : minutes;
    return hours + ':' + minutes + ' ' + ampm;
}
