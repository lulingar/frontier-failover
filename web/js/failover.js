var time_chart = seriesBarChart("#time-chart"),
    group_chart = dc.pieChart("#group-chart"),
    hosts_table = dc.dataTable("#hosts-table"),
    time_chart_width = time_chart.root()[0][0].parentElement.clientWidth,
    data_file = "test.csv",
    date_format = d3.time.format("%b %d, %Y %I:%M %p"),
    ndx, all, site_D, period, extent_span; 

var q = queue().defer(d3.json, "config.json")
               .defer(d3.csv, data_file);

q.await( function(error, config, dataset) {

    period = config.history.period;
    extent_span = 3.6e6 * config.history.span;

    var periodObj = minuteBunch(period),
        periodRange = periodObj.range,
        now = new Date(),
        this_hour = periodObj(now).getTime(),
        extent = [new Date(this_hour - extent_span), new Date(this_hour)],
        addH = function(p, d) { return p + d["Hits"]; },
        remH = function(p, d) { return p - d["Hits"]; },
        ini = function() { return 0; },
        sites_legend_item_size = 20,
        groups_base_dim = 150,
        groups_legend_width = 200;

    ndx = crossfilter( process_data(dataset));
    all = ndx.groupAll().reduce(addH, remH, ini);
    site_D = ndx.dimension( function(d) { return d["Sites"]; })

    var time_D = ndx.dimension( function(d) { return d["Timestamp"]; }),
        group_D = ndx.dimension( function(d) { return d["Group"]; }),
        hits_D = ndx.dimension(function(d){ return d["Hits"]; }),
        time_site_D = ndx.dimension(function(d) { return [d["Timestamp"], d["Sites"]]; }),
        group_G = group_D.group().reduce(addH, remH, ini),
        time_sites_G = time_site_D.group().reduce(addH, remH, ini),
        hits_G = hits_D.group().reduce(addH, remH, ini),
        site_list = site_D.group().all().map( function(d){ return d.key; }),
        num_sites = site_list.length,
        site_name_lengths = site_list.map( function(s){ return s.length; }),
        max_length = crossfilter.quicksort(site_name_lengths, 0, site_name_lengths.length)
                                .reverse()[0],
        sites_legend_space_v = (1 + num_sites) * sites_legend_item_size,
        sites_legend_space_h = 7*max_length,
        groups_radius = groups_base_dim/2 - 15;

    update_time_extent(period, extent_span);

    // The time series
    time_chart
      .width(1024)
      .height(415)
      .margins({top: 30, right: 30+sites_legend_space_h, bottom: 30, left: 60})
      .dimension(time_site_D)
      .group(time_sites_G)
      .seriesAccessor(function(d) { return d.key[1]; })
      .keyAccessor(function(d) { return d.key[0]; })
      .elasticY(true)
      .elasticX(true)
      .xAxisLabel("Time")
      .yAxisLabel("Hits")
      .x(d3.time.scale().domain(extent))
      .xUnits(periodRange)
      .renderHorizontalGridLines(true)
      .legend( dc.legend()
              .x( 1024-sites_legend_space_h ).y(10)
              .itemWidth(150).itemHeight(sites_legend_item_size)
              .gap(5) )
      .seriesSort(d3.ascending)
      .brushOn(false)
      .renderlet(function(chart) {
           chart.selectAll(".dc-legend-item")
                .on("click", function(d) { 
                    //TODO copy filtering functionality of pie chart
                    site_D.filterExact(d.name);
                    chart.select(".reset")
                         .style("display", null);
                    dc.redrawAll(); 
                 }); 
       });

    // The group chart
    group_chart.width(groups_base_dim)
               .height(groups_base_dim)
               .radius(groups_radius)
               .innerRadius(0.3*groups_radius)
               .dimension(group_D)
               .group(group_G)
               .ordinalColors(["#ff7f0e", "#17becf", "#2ca02c"])
               .label(function (d) {
                   if (group_chart.hasFilter() && !group_chart.hasFilter(d.key))
                        return "0%";
                    return (100 * d.value / all.value()).toFixed(2) + "%";
                })
               .renderlet( function(chart) {
                    draw_squids();
                })
               .legend( dc.legend().x(groups_base_dim).y(50).gap(10) );

    // Table widget for displaying failover details
    hosts_table.dimension(site_D)
               .group(function(d) { return d["Sites"]; })
               .columns([
                    function(d) { return d["Host"]; },
                    function(d) { return squid_place(d["IsSquid"]); },
                    function(d) { return d["Hits"]; },
                    function(d) { return size_natural(d["Bandwidth"]); },
                    function(d) { return date_format(d["Last visit"]); }
                    ])
               .sortBy(function(d) { return [d["Last visit"], d["Hits"]]; })
               .order(d3.descending)
               .size(Infinity)
               .on("filtered", function(chart, filter) {
                       draw_squids();
                       })
               .renderlet(function(table){
                    table.selectAll(".dc-table-group").classed("info", true);
               });

    // Draw all objects
    dc.renderAll();
});

function process_data(dataset) {
    var dataset = dataset;

    dataset.forEach( function(d) {
        d["Timestamp"] = new Date(+d["Timestamp"] * 1000);
        d["Last visit"] = new Date(+d["Last visit"] * 1000);
        d["Hits"] = +d["Hits"];
        d["HitsRate"] = +d["HitsRate"];
        d["Bandwidth"] = +d["Bandwidth"];
        d["BandwidthRate"] = +d["BandwidthRate"];
        d["IsSquid"] = (d["IsSquid"] == "True");
    });

    return dataset;
}

function squid_place (is_squid) {
    var spec = {true: "yes", false: "no"};
    return '<div class="squid-' + spec[is_squid] + '"></div>';
}

function draw_squids() {

    var spec = {  true: { selector: ".squid-yes",
                          text: "Yes",
                          color: "#3A9410" },
                 false: { selector: ".squid-no",
                          text: "No",
                          color: "#DE2810" } },
        width = 40, 
        height = 20;

    function draw_type(is_squid) {
        var d3image = d3.selectAll(spec[is_squid].selector),
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

    draw_type(true);
    draw_type(false);
}

function reload() {
    d3.csv( data_file, 
            function (error, dataset) {
                ndx.remove();
                ndx.add( process_data(dataset));
                dc.renderAll();
                update_time_extent(period, extent_span);
            } );
}

function update_time_extent(period, extent_span) {

    var periodObj = minuteBunch(period),
        periodRange = periodObj.range,
        now = new Date(),
        this_hour = periodObj(now).getTime(),
        extent = [new Date(this_hour - extent_span), new Date(this_hour)];

    // Show the currently plotted time span
    d3.select("#date-start")
      .attr("datetime", extent[0])
      .text(date_format(extent[0]));
    d3.select("#date-end")
      .attr("datetime", extent[1])
      .text(date_format(extent[1]));
}

function time_chart_reset() {
    site_D.filterAll();
    d3.select("#time-chart .reset")
      .style("display", "none");
    dc.redrawAll(); 
}
