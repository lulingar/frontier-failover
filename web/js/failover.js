//TODO make the reset transitions as smooth as when the pie chart is effectively cleared of filters (i.e. all slices are included)
//TODO gray-out all filtered-out items in a legend
//TODO make a reliable sorting of the hosts-table
//TODO add 1-hour ticks to the history chart ("time-chart")
//TODO add zoom capability to the history chart

var FailoverClass = function() {

    this.time_chart = seriesBarChart("#time-chart");
    this.group_chart = dc.pieChart("#group-chart");
    this.squid_chart = dc.pieChart("#squid-chart");
    this.hosts_table = dc.dataTable("#hosts-table");
    this.data_file = "failover.csv";
    this.date_format = d3.time.format("%b %d, %Y %I:%M %p");
    this.sites_legend_item_size = 20;
    this.groups_base_dim = 150;
    this.groups_legend_width = 200;
    this.groups_radius = this.groups_base_dim/2 - 15;

    this.start = function() {

        q = queue().defer(d3.json, "config.json")
                   .defer(d3.csv, data_file);

        q.await( function(error, config, dataset) {

            this.period = config.history.period;
            this.extent_span = 3.6e6 * config.history.span;

            this.periodObj = minuteBunch(period);
            this.periodRange = periodObj.range;
            this.now = new Date();
            this.this_hour = periodObj(now).getTime();
            this.extent = [new Date(this.this_hour - this.extent_span), new Date(this.this_hour)];
            this.addH = function(p, d) { return p + d["Hits"]; };
            this.remH = function(p, d) { return p - d["Hits"]; };
            this.ini = function() { return 0; };

            this.ndx = crossfilter( this.process_data(dataset));
            this.all = this.ndx.groupAll().reduce(this.addH, this.remH, this.ini);
            this.site_D = this.ndx.dimension( function(d) { return d["Sites"]; })

            this.time_D = this.ndx.dimension( function(d) { return d["Timestamp"]; });
            this.group_D = this.ndx.dimension( function(d) { return d["Group"]; });
            this.squid_D = this.ndx.dimension( function(d) { 
                                        var host_type = { true: "Squid",
                                                          false: "Worker Node" };
                                        return host_type[d["IsSquid"]]; 
                                    });
            this.hits_D = this.ndx.dimension(function(d){ return d["Hits"]; });
            this.time_site_D = this.ndx.dimension(function(d) { return [d["Timestamp"], d["Sites"]]; });
            this.group_G = this.group_D.group().reduce(this.addH, this.remH, this.ini);
            this.squid_G = this.squid_D.group().reduce(this.addH, this.remH, this.ini);
            this.time_sites_G = this.time_site_D.group().reduce(this.addH, this.remH, this.ini);
            this.hits_G = this.hits_D.group().reduce(this.addH, this.remH, this.ini);
            this.site_list = this.site_D.group().all().map( function(d){ return d.key; });
            this.num_sites = this.site_list.length;
            this.site_name_lengths = this.site_list.map( function(s){ return s.length; });
            this.max_length = crossfilter.quicksort(this.site_name_lengths, 0, this.site_name_lengths.length)
                                    .reverse()[0];
            this.sites_legend_space_v = (1 + this.num_sites) * this.sites_legend_item_size;
            this.sites_legend_space_h = 7*this.max_length;

            this.update_time_extent(this.period, this.extent_span);

            // The time series
            this.time_chart.width(1024)
                      .height(415)
                      .margins({top: 30, right: 30+this.sites_legend_space_h, bottom: 30, left: 60})
                      .dimension(this.time_site_D)
                      .group(this.time_sites_G)
                      .seriesAccessor(function(d) { return d.key[1]; })
                      .keyAccessor(function(d) { return d.key[0]; })
                      .title(function(d) { return d.key[1] + ": " + d.value + " Hits"; })
                      .elasticY(true)
                      .elasticX(true)
                      .xAxisLabel("Time")
                      .yAxisLabel("Hits")
                      .x(d3.time.scale().domain(this.extent))
                      .xUnits(this.periodRange)
                      .renderHorizontalGridLines(true)
                      .legend( dc.legend()
                                 .x( 1024-this.sites_legend_space_h ).y(10)
                                 .itemWidth(150).itemHeight(this.sites_legend_item_size)
                                 .gap(5) )
                      .seriesSort(d3.ascending)
                      .brushOn(false)
                      .renderlet(function(chart) {
                          chart.selectAll(".dc-legend-item")
                                  .on("click", function(d) { 
                                      //TODO copy filtering functionality of pie chart
                                      this.site_D.filterExact(d.name);
                                      chart.select(".reset")
                                           .style("display", null);
                                      dc.redrawAll(); 
                                  }); 
                      });

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
                        })
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
                        })
                    .legend( dc.legend().x(this.groups_base_dim).y(50).gap(10) )
                    .renderlet( function(chart) {
                            this.draw_squids();
                        });

            // Table widget for displaying failover details
            this.hosts_table.dimension(this.site_D)
                    .group(function(d) { return d["Sites"]; })
                    .columns([
                            function(d) { 
                                var host = d["Host"],
                                    alias = ( d["Alias"] === '' ? host : d["Alias"] );
                                return '<span title="Host: ' + host + '">' + alias + '</span>'; 
                            },
                            function(d) { return this.squid_place(d["IsSquid"]); },
                            function(d) { return this.date_format(d["Timestamp"]); },
                            function(d) { return d["Hits"]; },
                            function(d) { return size_natural(d["Bandwidth"]); },
                            function(d) { return size_natural(d["BandwidthRate"]) + "/s"; }
                            ])
                    .sortBy(function(d) { return [d["Timestamp"], d["Hits"]]; })
                    .order(d3.descending)
                    .size(Infinity)
                    .on("filtered", function(chart, filter) {
                            this.draw_squids();
                            })
                    .renderlet(function(table){
                            table.selectAll(".dc-table-group").classed("info", true);
                    });

            // Draw all objects
            dc.renderAll();
        });
    }

    this.process_data = function(dataset) {
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

    this.squid_place = function(is_squid) {
        var spec = {true: "yes", false: "no"};
        return '<div class="squid-' + spec[is_squid] + '"></div>';
    }

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
    }

    this.reload = function() {
        d3.csv( this.data_file, 
                function (error, dataset) {
                    this.ndx.remove();
                    this.ndx.add( this.process_data(dataset));
                    dc.renderAll();
                    this.update_time_extent(this.period, this.extent_span);
                } );
    }

    this.update_time_extent = function(period, extent_span) {

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

    this.time_chart_reset = function() {
        site_D.filterAll();
        d3.select("#time-chart .reset")
          .style("display", "none");
        dc.redrawAll(); 
    }
}

var Failover = new FailoverClass();
Failover.start();
