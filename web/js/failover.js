var time_chart = seriesBarChart("#time-chart"),
    hosts_table = dc.dataTable("#hosts-table"),
    width = time_chart.root()[0][0].parentElement.clientWidth;

var q = queue().defer(d3.json, "config.json")
               .defer(d3.csv, "test.csv");

q.await( function(error, config, dataset) {

    var period = config.history.period,
        periodObj = minuteBunch(period),
        periodRange = periodObj.range,
        now = new Date(),
        this_hour = periodObj(now).getTime(),
        extent_span = 3.6e6 * config.history.span,
        extent = [new Date(this_hour - extent_span), new Date(this_hour)],
        addH = function(p, d) { return p + d["Hits"]; },
        remH = function(p, d) { return p - d["Hits"]; },
        ini = function() { return 0; },
        legend_item_size = 20;

    dataset.forEach( function(d) {
        d["Timestamp"] = new Date(+d["Timestamp"] * 1000);
        d['Last visit'] = new Date(+d['Last visit'] * 1000);
        d["Hits"] = +d["Hits"];
        d["HitsRate"] = +d["HitsRate"];
        d["Bandwidth"] = +d["Bandwidth"];
        d["BandwidthRate"] = +d["BandwidthRate"];
        d["IsSquid"] = (d["IsSquid"] == "True");
    });

    var ndx = crossfilter(dataset),
        time = ndx.dimension( function(d) { return d["Timestamp"]; }),
        site = ndx.dimension( function(d) { return d["Sites"]; }),
        site_list = site.group().all().map( function(d){ return d.key; }),
        num_sites = site_list.length,
        site_name_lengths = site_list.map( function(s){ return s.length; }),
        max_length = crossfilter.quicksort(site_name_lengths, 0, site_name_lengths.length)
                                .reverse()[0],
        legend_space_v = (1 + num_sites) * legend_item_size,
        legend_space_h = 7*max_length,
        time_site = ndx.dimension(function(d) { return [d["Timestamp"], d["Sites"]]; }),
        time_sites = time_site.group().reduce(addH, remH, ini),
        hits = ndx.dimension(function(d){ return d["Hits"]; }),
        hitsCounts = hits.group().reduce(addH, remH, ini),
        date_format = d3.time.format("%b %d, %Y %I:%M %p");

    // Display the currently plotted time span
    d3.select("#date-start")
      .attr("datetime", extent[0])
      .text(date_format(extent[0]));
    d3.select("#date-end")
      .attr("datetime", extent[1])
      .text(date_format(extent[1]));

    time.filterRange(extent);

    // The time series
    time_chart
      .width(1024)
      .height(415)
      .margins({top: 30, right: 30+legend_space_h, bottom: 30, left: 60})
      .dimension(time_site)
      .group(time_sites)
      .seriesAccessor(function(d) { return d.key[1]; })
      .keyAccessor(function(d) {return d.key[0];})
      .elasticY(true)
      .elasticX(true)
      .xAxisLabel("Time")
      .yAxisLabel("Hits")
      .x(d3.time.scale().domain(extent))
      .xUnits(periodRange)
      .renderHorizontalGridLines(true)
      .legend( dc.legend()
              .x( 1024-legend_space_h )
              .y(10)
              .itemHeight(legend_item_size).itemWidth(150)
              .gap(5) )
      .seriesSort(d3.ascending)
      .brushOn(false);

    // Table widget for displaying failover details
    hosts_table.dimension(site)
               .group(function(d) { return d["Sites"]; })
               .columns([
                    function(d) { return d["Host"]; },
                    function(d) { return squid_place(d["IsSquid"]); },
                    function(d) { return d["Hits"]; },
                    function(d) { return size_natural(d["Bandwidth"]); },
                    function(d) { return date_format(d['Last visit']); }
                    ])
               .sortBy(function(d) { return [d['Last visit'], d["Hits"]]; })
               .order(d3.descending)
               .size(Infinity)
               .renderlet(function(table){
                    table.selectAll(".dc-table-group").classed("info", true);
               });

    // Draw all objects
    dc.renderAll();
    draw_squids(true);
    draw_squids(false);
});

function squid_place (is_squid) {
    var spec = {true: "yes", false: "no"};
    return '<div class="squid-' + spec[is_squid] + '"></div>';
}

function draw_squids (is_squid) {

    var spec = {  true: { selector: ".squid-yes",
                          text: "Yes",
                          color: "#3A9410" },
                 false: { selector: ".squid-no",
                          text: "No",
                          color: "#DE2810" } },
        width = 40, 
        height = 20,
        d3image = d3.selectAll(spec[is_squid].selector),
        svgcanvas = d3image.append("svg:svg")
                           .attr("width", width)
                           .attr("height", height);

    svgcanvas.append("svg:rect")
             .attr("x",0)
             .attr("y",0)
             .attr("width", width)
             .attr("height", height)
             .style("fill", spec[is_squid].color),
    svgcanvas.append("svg:text")
             .text(spec[is_squid].text)    
             .attr("x", width/2)
             .attr("y", height/2)
             .attr("text-anchor", "middle")
             .attr("dominant-baseline", "central")
             .style("fill", "white");
}

