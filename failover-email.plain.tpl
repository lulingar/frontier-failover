This is an automated message.  Many database queries from your site have directly connected to the following Frontier Server Groups during the last hour, with a high rate of queries not going through your local squid(s).
{server_groups}

The record of Frontier activity from your site during the last period ({period:d} minutes) is displayed below. The full access history during the past {record_span:d} hours is available at http://wlcg-squid-monitor.cern.ch/fftest2/failover.html?site={site_query_url} .

The most common sources of this problem are:
    1. Squids are not running
    2. Squids are not listed in site-local-config.xml
    3. Not all local IP addresses are in squid's access control lists

When you have found the cause of the problem or if you have any questions, 
please contact {support_email} by replying to this message.

[*] The rate is the effective number of queries per second over each period.

===== Record of Frontier server accesses in the last hour =====
Site: {site_name} 
{summary_table}

