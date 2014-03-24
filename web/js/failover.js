//TODO make a reliable sorting of the hosts-table
//TODO add zoom capability to the history chart

var Failover = new function() {

    var scope = this;
    // JS weirdness: You have to append ".bind(scope)" at the end of every
    //  member function, after its closing brace, in order
    //  to make sure any instances of "this" within the function actually
    //  reference the object, and not the Global scope (a.k.a. "Window")

    this.time_chart = dc.seriesChart("#time-chart");
    this.time_chart_range = dc.barChart("#time-range-chart");
    this.group_chart = dc.pieChart("#group-chart");
    this.squid_chart = dc.pieChart("#squid-chart");
    this.hosts_table = dc.dataTable("#hosts-table");
    this.data_file = "failover.csv";
    this.date_format = d3.time.format("%b %d, %Y %I:%M %p");
    this.sites_legend_item_size = 20;
    this.groups_base_dim = 150;
    this.groups_legend_width = 200;
    this.groups_radius = this.groups_base_dim/2 - 15;

    this.setup = function(error, config, dataset) {

        this.config = config;
        this.period = config.history.period;
        this.extent_span = 3.6e6 * config.history.span;

        this.periodObj = minuteBunch(this.period);
        this.periodRange = this.periodObj.range;
        this.addH = function(p, d) { return p + d.Hits; };
        this.remH = function(p, d) { return p - d.Hits; };
        this.ini = function() { return 0; };

        this.raw_data = this.process_data(dataset);
        this.ndx = crossfilter(this.raw_data);
        this.all = this.ndx.groupAll().reduce(this.addH, this.remH, this.ini);
        this.site_D = this.ndx.dimension( function(d) { return d.Sites; })

        this.time_D = this.ndx.dimension( function(d) { return d.Timestamp; });
        this.group_D = this.ndx.dimension( function(d) {
                                    return this.config.groups[d.Group].name; 
                                }.bind(scope));
        this.squid_D = this.ndx.dimension( function(d) { 
                                    var host_type = { true: "Squid",
                                                      false: "Worker Node" };
                                    return host_type[d.IsSquid]; 
                                });
        this.hits_D = this.ndx.dimension( function(d) { return d.Hits; });
        this.time_site_D = this.ndx.dimension( function(d) { return [d.Timestamp, d.Sites]; });
        this.group_G = this.group_D.group().reduce(this.addH, this.remH, this.ini);
        this.squid_G = this.squid_D.group().reduce(this.addH, this.remH, this.ini);
        this.time_sites_G = this.time_site_D.group().reduce(this.addH, this.remH, this.ini);
        this.time_G = this.time_D.group().reduce(this.addH, this.remH, this.ini);
        this.hits_G = this.hits_D.group().reduce(this.addH, this.remH, this.ini);
        this.site_list = this.site_D.group().all().map( function(d) { return d.key; });
        this.num_sites = this.site_list.length;
        this.site_name_lengths = this.site_list.map( function(s) { return s.length; });
        this.max_length = crossfilter.quicksort(this.site_name_lengths, 0, this.site_name_lengths.length)
                                .reverse()[0];
        this.sites_legend_space_v = (1 + this.num_sites) * this.sites_legend_item_size;
        this.sites_legend_space_h = 7*this.max_length;

        this.update_time_extent(this.period, this.extent_span);

        // The time series
        var time_chart_width = 1024;
        this.time_chart.width(time_chart_width)
                  .height(415)
                  .chart( function(c) { return dc.barChart(c) } )
                  .margins({top: 30, right: 30+this.sites_legend_space_h, bottom: 40, left: 60})
                  .dimension(this.time_site_D)
                  .group(this.time_sites_G)
                  .keyAccessor(function(d) { return d.key[0]; })
                  .seriesAccessor(function(d) { return d.key[1]; })
                  .seriesSort(d3.descending)
                  .title(function(d) { return d.key[1] + ": " + d.value + " Hits"; })
                  .yAxisLabel("Hits")
                  .mouseZoomable(true)
                  .rangeChart(this.time_chart_range)
                  .elasticY(true)
                  .x(d3.time.scale().domain(this.extent))
                  .xUnits(this.periodRange)
                  .renderHorizontalGridLines(true)
                  .legend( dc.legend()
                             .x( 1024-this.sites_legend_space_h ).y(10)
                             .itemWidth(150).itemHeight(this.sites_legend_item_size)
                             .gap(5) )
                  .brushOn(false)
                  .renderlet(function(chart) {
                      chart.selectAll(".dc-legend-item")
                           .on("click", function(d) { 
                                          this.site_D.filterExact(d.name);
                                          chart.turnOnControls();
                                          dc.redrawAll(); 
                                       }.bind(scope) ); 
                   });

        this.time_chart.xAxis().ticks(d3.time.hours, 2);

        // Add listeners to rotate labels and refresh data table
        var rotate_fun = function(d) { 
                return "rotate(-90) translate(-25, -12)"; 
            };
        var axis_tick_rotate = function(chart) { 
                chart.selectAll("svg g g.axis.x g.tick text")
                     .attr("transform", rotate_fun);
            }
        this.time_chart.on("postRedraw", axis_tick_rotate);
        this.time_chart.on("postRender", axis_tick_rotate);
       
        // the time range controller chart 
        this.time_chart_range.width(time_chart_width)
                  .height(80)
                  .margins({top: 0, right: 30+this.sites_legend_space_h, bottom: 60, left: 70})
                  .dimension(this.time_D)
                  .group(this.time_G)
                  .x(d3.time.scale().domain(this.extent))
                  .xUnits(this.periodRange)
                  .elasticY(true)
                  .gap(1);
        this.time_chart_range.xAxis().ticks(d3.time.hours, 2);
        this.time_chart_range.on("postRedraw", axis_tick_rotate);
        this.time_chart_range.on("postRender", axis_tick_rotate);

        // The group chart
        this.group_chart.width(this.groups_base_dim)
                .height(this.groups_base_dim)
                .radius(this.groups_radius)
                .innerRadius(0.3*this.groups_radius)
                .dimension(this.group_D)
                .group(this.group_G)
                .ordinalColors(["#ff7f0e", "#17becf", "#2ca02c"])
                .title(function(d) { return d.value + " Hits"; })
                .label(function (d) {
                    if (this.group_chart.hasFilter() && !this.group_chart.hasFilter(d.key))
                            return "0%";
                        return (100 * d.value / this.all.value()).toFixed(2) + "%";
                    }.bind(scope))
                .legend( dc.legend().x(this.groups_base_dim).y(50).gap(10) );

        // The host (squid/not squid) chart
        this.squid_chart.width(this.groups_base_dim)
                .height(this.groups_base_dim)
                .radius(this.groups_radius)
                .innerRadius(0.3*this.groups_radius)
                .dimension(this.squid_D)
                .group(this.squid_G)
                .title(function(d) { return d.value + " Hits"; })
                .label(function (d) {
                    if (this.squid_chart.hasFilter() && !this.squid_chart.hasFilter(d.key))
                            return "0%";
                        return (100 * d.value / this.all.value()).toFixed(2) + "%";
                    }.bind(scope))
                .legend( dc.legend().x(this.groups_base_dim).y(50).gap(10) )
                .renderlet( function(chart) {
                        this.draw_squids();
                    }.bind(scope));

        // Table widget for displaying failover details
        this.hosts_table.dimension(this.site_D)
                .group(function(d) { return d.Sites; })
                .columns([
                        function(d) { 
                            var host = d.Host,
                                alias = ( d.Alias === '' ? host : d.Alias );
                            return '<span title="Host: ' + host + '">' + alias + '</span>'; 
                        },
                        function(d) { return this.squid_place(d.IsSquid); }.bind(scope),
                        function(d) { return this.date_format(d.Timestamp); }.bind(scope),
                        function(d) { return d.Hits; },
                        function(d) { return size_natural(d.Bandwidth); },
                        function(d) { return size_natural(d.BandwidthRate) + "/s"; }
                        ])
                .sortBy(function(d) { return [d.Timestamp, d.Hits]; })
                .order(d3.descending)
                .size(Infinity)
                .on("filtered", function(chart, filter) {
                        this.draw_squids();
                        }.bind(scope))
                .renderlet(function(table){
                        table.selectAll(".dc-table-group").classed("info", true);
                });

        // Draw all objects
        dc.renderAll();
    }.bind(this);

    this.start = function() {
        var q, proxy = this.setup.bind(this);

        q = queue().defer(d3.json, "config.json")
                   .defer(d3.csv, this.data_file);
        q.await(proxy);
    }.bind(this);

    this.process_data = function(dataset) {
        var dataset = dataset;

        dataset.forEach( function(d) {
            // The timestamp points to the end of a period. 
            //  this must be accounted for for plotting.
            d.Timestamp = new Date((+d.Timestamp - 3600) * 1000);

            d.Hits = +d.Hits;
            d.HitsRate = +d.HitsRate;
            d.Bandwidth = +d.Bandwidth;
            d.BandwidthRate = +d.BandwidthRate;
            d.IsSquid = (d.IsSquid == "True");
        });

        return dataset;
    }.bind(this);

    this.squid_place = function(is_squid) {
        var spec = {true: "yes", false: "no"};
        return '<div class="squid-' + spec[is_squid] + '"></div>';
    }.bind(this);

    this.draw_squids = function() {

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
    }.bind(this);

    this.reload = function() {
        d3.csv( this.data_file, 
                function (error, dataset) {
                    this.raw_data = this.process_data(dataset);

                    this.ndx.remove();
                    this.ndx.add( this.raw_data);

                    dc.renderAll();
                    this.update_time_extent(this.period, this.extent_span);
                }.bind(scope) );
    }.bind(this);

    this.update_time_extent = function(period, extent_span) {

        var periodObj = minuteBunch(period),
            periodRange = periodObj.range,
            hour = 3.6e6,
            now = new Date(),
            this_hour = periodObj(now).getTime(),
            extent = [new Date(this_hour - extent_span),
                      new Date(this_hour)],
            extent_pad = [new Date(this_hour - extent_span - hour),
                          new Date(this_hour + hour)];

        // Show the currently plotted time span
        d3.select("#date-start")
          .attr("datetime", extent[0])
          .text(this.date_format(extent[0]));
        d3.select("#date-end")
          .attr("datetime", extent[1])
          .text(this.date_format(extent[1]));

        this.extent = extent_pad;

    }.bind(this);

    this.time_chart_reset = function() {
        this.site_D.filterAll();
        this.time_chart.turnOffControls();
        dc.redrawAll(); 
    }.bind(this);
}

Failover.start();
