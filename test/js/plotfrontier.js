'use strict';

// ### Create Chart Objects
var fluctuationChart = dc.barChart("#fluctuation-chart");
//var moveChart = dc.lineChart("#monthly-move-chart");

d3.csv("summary.csv", function (data) {
    var dateFormat = d3.time.format("%m/%d/%Y");
    var numberFormat = d3.format(".2f");

    data.forEach(function (d) {
        d.dd = Date(d.Time);
        d.q_sqd = +d["Squid queries"];
        d.q_dir = +d["Direct queries"];
    });

    //### Create Crossfilter Dimensions and Groups
    var ndx = crossfilter(data);
    var all = ndx.groupAll();

    // dimension by timestamp 
    var dateDimension = ndx.dimension(function (d) {
        return d.dd;
    });

    // determine a histogram of percent changes
    var fluctuation = ndx.dimension(function (d) {
        return d.q_sqd;
    });
    var fluctuationGroup = fluctuation.group();

    //TODO: Understand how does the x axis data gets set 
    //#### Bar Chart
    /* dc.barChart("#volume-month-chart") */
    fluctuationChart
        .width(990)
        .height(200)
        .margins({top: 30, right: 50, bottom: 25, left: 40})
        .dimension(fluctuation)
        .group(fluctuationGroup)
        .elasticY(true)
        .centerBar(true)
        .gap(1)
        .x(d3.time.scale().domain([new Date(2013, 12, 10), new Date(2013, 12, 25)]))
        .renderHorizontalGridLines(true);

    // Customize axis
    fluctuationChart.xAxis().tickFormat(
        function (v) { return v + "%"; });
    fluctuationChart.yAxis().ticks(5);

    /*
    //#### Data Count
    // Create a data count widget and use the given css selector as anchor. You can also specify
    // an optional chart group for this chart to be scoped within. When a chart belongs
    // to a specific group then any interaction with such chart will only trigger redraw
    // on other charts within the same chart group.
    <div id="data-count">
        <span class="filter-count"></span> selected out of <span class="total-count"></span> records
    </div>
    */
    dc.dataCount(".dc-data-count")
        .dimension(ndx)
        .group(all);


    //#### Rendering
    //simply call renderAll() to render all charts on the page
    dc.renderAll();
    /*
    // or you can render charts belong to a specific chart group
    dc.renderAll("group");
    // once rendered you can call redrawAll to update charts incrementally when data
    // change without re-rendering everything
    dc.redrawAll();
    // or you can choose to redraw only those charts associated with a specific chart group
    dc.redrawAll("group");
    */
});

//#### Version
//Determine the current version of dc with `dc.version`
d3.selectAll("#version").text(dc.version);
