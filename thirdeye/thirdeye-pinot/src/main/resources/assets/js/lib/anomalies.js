function getAnomalies() {

    //Todo: add the real endpoint
//    var url = "/api/anomaly-results/collection/thirdeyeAbook"
//    getData(url).done(function(data) {
//        renderAnomalies(data);
//    });
    var data = [
        {
            id: 53,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462172400000,
            endTimeUtc: 1462215600000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0,
            weight: -0.2681636837740775,
            properties: "max_likelihood=42.15392904017426;complementary_pattern=UP;target_level=0.0;insert_time=1462233622234;complementary_level=0.1;target_pattern=DOWN",
            message: null,
            creationTimeUtc: 1462233622234,
            filters: null
        },
        {
            id: 54,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462287600000,
            endTimeUtc: 1462287600000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0,
            weight: -0.8483997471588596,
            properties: "max_likelihood=30.219873846539485;complementary_pattern=UP;target_level=0.0;insert_time=1462320022157;complementary_level=0.1;target_pattern=DOWN",
            message: null,
            creationTimeUtc: 1462320022157,
            filters: null
        },
        {
            id: 55,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462287600000,
            endTimeUtc: 1462287600000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0,
            weight: -0.7558306331988652,
            properties: "max_likelihood=22.232265178853595;complementary_pattern=UP;target_level=0.0;insert_time=1462406422563;complementary_level=0.1;target_pattern=DOWN",
            message: null,
            creationTimeUtc: 1462406422563,
            filters: null
        },
        {
            id: 56,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462287600000,
            endTimeUtc: 1462287600000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0,
            weight: -0.7490763196026473,
            properties: "max_likelihood=21.393158898360525;complementary_pattern=UP;target_level=0.0;insert_time=1462492823389;complementary_level=0.1;target_pattern=DOWN",
            message: null,
            creationTimeUtc: 1462492823389,
            filters: null
        },
        {
            id: 57,
            functionId: 2,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_complementary_up;targetPattern=UP;complementaryPattern=DOWN;targetLevel=0.1;complementaryLevel=0.0;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462237200000,
            endTimeUtc: 1462284000000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0.006,
            weight: 0.13456578903297278,
            properties: "max_likelihood=7.555141976430605;complementary_pattern=DOWN;target_level=0.1;insert_time=1462492823390;complementary_level=0.0;target_pattern=UP",
            message: null,
            creationTimeUtc: 1462492823390,
            filters: null
        },
        {
            id: 58,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462449600000,
            endTimeUtc: 1462564800000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0.002,
            weight: -0.07167628495693656,
            properties: "max_likelihood=8.993432234457941;complementary_pattern=UP;target_level=0.0;insert_time=1462579224697;complementary_level=0.1;target_pattern=DOWN",
            message: "Severity: -0.072, score: 0.002",
            creationTimeUtc: 1462579224697,
            filters: null
        },
        {
            id: 59,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462449600000,
            endTimeUtc: 1462474800000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0.002,
            weight: -0.13348671417334246,
            properties: "max_likelihood=9.689816953246918;complementary_pattern=UP;target_level=0.0;insert_time=1462665623686;complementary_level=0.1;target_pattern=DOWN",
            message: "Severity: -0.133, score: 0.002",
            creationTimeUtc: 1462665623686,
            filters: null
        },
        {
            id: 60,
            functionId: 1,
            functionType: "SCAN_STATISTICS",
            functionProperties: "functionName=abook_scan_target_down;targetPattern=DOWN;complementaryPattern=UP;targetLevel=0.0;complementaryLevel=0.1;seasonal=168;numSimulations=500;minWindowLength=1;bootstrap=false;maxWindowLength=100000;enableSTL=true;pValueThreshold=0.01;monitoringWindow=72",
            collection: "thirdeyeAbook",
            startTimeUtc: 1462626000000,
            endTimeUtc: 1462737600000,
            dimensions: "*,*,*,*,*,*,*,*,*,*,*",
            metric: "guestInvitationsSubmitted",
            score: 0,
            weight: -0.07062230128944169,
            properties: "max_likelihood=10.956304526792337;complementary_pattern=UP;target_level=0.0;insert_time=1462752020417;complementary_level=0.1;target_pattern=DOWN",
            message: "Severity: -0.071, score: 0.000",
            creationTimeUtc: 1462752020417,
            filters: null
        }
    ]

    renderAnomalies(data);
};

function renderAnomalies(data) {


    console.log("anomalies data")
    console.log(data)

    /* Handelbars template for funnel table */
    var result_anomalies_template = HandleBarsTemplates.template_anomalies(data);
    $("#"+ hash.view +"-display-chart-section").append(result_anomalies_template);

}
