minuteBunch = function(numMinutes) {
    var bunch_ms = numMinutes * 6e4;

    return d3.time.interval( 
	function(date) {
	    var timezone = date.getTimezoneOffset();
	    return new Date(Math.floor( (date )/bunch_ms ) * bunch_ms);
	}, 
	function(date, offset) {
	    date.setTime(date.getTime() + Math.floor(offset) * bunch_ms); // DST breaks setMinutes
	}, 
        function(date) {
            var minutes = date.getMinutes();
            var number = Math.floor(minutes / numMinutes) * numMinutes;
	    return number;
	}
    );
}

function size_natural (size) 
{
    if (size == 0) return "0 B";

    var scales = {'B': 0, 'kiB': 10, 'MiB': 20, 'GiB': 30, 'TiB': 40};

    var proportion = []
    for (var key in scales) {
        proportion.push([Math.abs(1 - ((Math.pow(2, scales[key])) / size)), key]);
    }
    proportion.sort();
    var scale = proportion[0][1];

    var base_part = size / Math.pow(2, scales[scale]);
    var size_str = base_part.toFixed(2) + ' ' + scale; 

    return size_str;
}

function flatten_array (arr)
{
    var flat = [];
    flat = flat.concat.apply(flat, arr);
    return flat;
}

/* 
  Generates a set of CSS color specifications
  based on the HSL model, by uniformly dividing
  the hue wheel at constant saturation and luminosity.

  Params:

    num_colors  The amount of colors to generate
    sat         The common saturation value [0-100]
    lum         The common luminosity value [0-100]
    [hue_start] The starting point in the Hue wheel [0-360)
*/
function hsl_set (num_colors, sat, lum, hue_start)
{
    var colors = [],
        hue_step = 360/num_colors, 
        h;

    if (typeof(hue_start) === 'undefined') hue_start = 0;

    for (var i = 0; i < num_colors; i++) {
        h = (hue_start + i*hue_step) % 360;
        colors.push('hsl(' +h+ ',' +sat+ '%,' +lum+ '%)')
    }

    if (num_colors == 1) return colors[0];
    else return colors;
}

/* Parses simple arguments passed in the URL */
function getUrlVars()
{
    var vars = [],
        hash,
        arg_start = window.location.href.indexOf('?'),
        hashes;
   
    if (arg_start > 0) {
        hashes = window.location.href
                                .slice(arg_start + 1)
                                .split('&');

        for(var i = 0; i < hashes.length; i++)
        {
            hash = hashes[i].split('=');
            vars.push(hash[0]);
            vars[hash[0]] = decodeURIComponent(hash[1]);
        }
    }

    return vars;
}
