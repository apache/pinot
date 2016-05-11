    /**--- 4) Eventlisteners ---**/

    /** Dashboard Form related eventlisteners **/

    //Prevent jumping view when anchors are clicked
    $("#main-view").on("click","a[href='#']", function(event){
        eventPreventDefault(event)
    });


    //Radio selection will highlight selected element
    $("#main-view").on("click", ".radio-options",function(){
        radioOptions(this)
    });

    $("#main-view").on("click",".radio-buttons button",function(){
        radioButtons(this)
    });

    /** Dataset selection **/
    $("#main-view").on("click",".dataset-option", function(){
        selectDatasetNGetDashboardList(this)
    });

    //Advanced settings
    $("#main-view").on("click",".close-dropdown-btn", function(){
        closeClosestDropDown(this)
    });

    /* Form tab selection */

    $("#main-view").on("click",".header-tab", function(){
        switchHeaderTab(this)
    });

    /** Dashboard selection **/
    $("#main-view").on("click",".dashboard-option", function(){
        selectDashboard(this)
    });

    /** Metric selection **/
    $("#main-view").on("click",".metric-option", function(){
       selectMetric(this)
    })

    /** Dimension selection **/
    $("#main-view").on("click",".dimension-option", function(){
       selectDimension(this)
    })

    /** Dimension select all **/
    $("#main-view").on("click",".dropdown-toggle-all", function(){
        toggleAllDimensions(this)
    })

    /** Remove selected Metric or Dimension item **/
    $("#main-view").on("click",".remove-selection", function(){
        removeSelection(this)
    });
