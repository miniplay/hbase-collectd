import subprocess
import os
import time
import collectd

HBASE_METRICS_TMP_FILE = "/tmp/hbase_metrics.tmp"
HBASE_SIZE_TMP_FILE = "/tmp/hbase_size.tmp"
HBASE_PORT = "60020" # HBASE Port
HBASE_DOMAIN_DNS = ".VPN.EXAMPLE.COM" # PARENT DOMAIN FOR HBASE NODES (example: hbase01.vpn.example.com)

collectd_plugin_name = "hbase"
collectd_hostname = "hbase01" # Where do you want to report the global metrics
collectd_interval = 30
collectd_type = "gauge"

def wait_for_collection():
	while True:
		if os.path.isfile(HBASE_METRICS_TMP_FILE) and 'load' in open(HBASE_METRICS_TMP_FILE).read():
			break
		else:
			collectd.info("Sleeping...")
			time.sleep(1)

def hbase_status():
	# Block until stats are written to file
	wait_for_collection()

	metrics = {}

	grep_alive_servers = "grep live "+HBASE_METRICS_TMP_FILE+" | awk {'print $1'}"
	grep_alive_cmd = subprocess.Popen(grep_alive_servers,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

	grep_dead_servers = "grep dead "+HBASE_METRICS_TMP_FILE+" | awk {'print $1'}"
	grep_dead_cmd = subprocess.Popen(grep_dead_servers,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)


	awk_nodes_info_cmd = "awk '/dead/{f=0} f; /live/{f=1}' /root/test.tmp"
	awk_nodes_info_cmd = subprocess.Popen(awk_nodes_info_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

	nodes_info_output = awk_nodes_info_cmd.communicate()[0]

	nodes_info_output = nodes_info_output.split('\n')
	nodes_stats = {}
	for idx, cmd_line in enumerate(nodes_info_output):
	        host = ""
	        stats = ""
	        if HBASE_DOMAIN_DNS in cmd_line:
	                host = cmd_line.split(HBASE_DOMAIN_DNS+":"+HBASE_PORT)[0]
	                host = host.strip()
	                nodes_stats[host] = {}
	                stats = nodes_info_output[idx + 1]
	                stats = stats.split(", ")
	                for stat in stats:
        	                stat = stat.split("=")
                	        stat_key = stat[0].strip()
	                        stat_value = stat[1].strip()
	                        nodes_stats[host][stat_key] = stat_value


	nodes_alive = grep_alive_cmd.communicate()[0]
	nodes_dead = grep_dead_cmd.communicate()[0]

	metrics["alive"] = nodes_alive
	metrics["dead"] = nodes_dead
	metrics["nodes"] = nodes_stats
	collectd.info("Alive Servers " + nodes_alive + " Dead Servers " + nodes_dead)

	return metrics

def hbase_disk_usage():
	# Block until stats are written to file
	wait_for_collection()

	metrics = {}

	with open(HBASE_SIZE_TMP_FILE) as fp:
	    for line in fp:
		splitted_line = line.split()
		table_name = splitted_line[1].replace("/hbase/","")
		table_size = splitted_line[0]

		if table_name[:1] == '.' or table_name[:1] == '-':
			continue
		#collectd.info("Table [" + table_name + "] has size " + table_size)
		metrics[table_name] = table_size
	return metrics

def remove_temp_files():
	try:
	    os.remove(HBASE_METRICS_TMP_FILE)
	    os.remove(HBASE_SIZE_TMP_FILE)
	except OSError:
	    pass

def write_tmp_hbase_stats():
	hbase_size_cmd = "hadoop fs -du /hbase/ 1> "+HBASE_SIZE_TMP_FILE+" 2> /dev/null"
	subprocess.Popen(hbase_size_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

	hbase_status_cmd = "echo \"status 'simple'\" | hbase shell 1> "+HBASE_METRICS_TMP_FILE+" 2> /dev/null"
	subprocess.Popen(hbase_status_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

	time.sleep(5)

def restore_sigchld():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

def read_callback(data=None):
	collectd.info("Hbase collector starting...")

	write_tmp_hbase_stats()

	status_metrics = hbase_status()
	disk_usage_metrics = hbase_disk_usage()

	if not disk_usage_metrics:
		pass
	else:
        	for key in disk_usage_metrics:
	            metric = collectd.Values()
	            metric.plugin = collectd_plugin_name+"_table-usage"
	            metric.host = collectd_hostname
	            metric.interval = collectd_interval
	            metric.type = collectd_type
	            metric.type_instance = key
	            metric.values = [disk_usage_metrics[key]]
	            metric.dispatch()

	if not status_metrics:
		pass
	else:
        	for key in status_metrics:
		    if key != 'nodes':
				metric = collectd.Values()
				metric.plugin = collectd_plugin_name+"_status"
		         	metric.host = collectd_hostname
				metric.interval = collectd_interval
				metric.type = collectd_type
				metric.type_instance = key
				metric.values = [int(status_metrics[key])]
				metric.dispatch()
		    else:
				for node_key in status_metrics[key]:
					for stat_key in status_metrics[key][node_key]:
						metric = collectd.Values()
		                                metric.plugin = collectd_plugin_name+"_nodestatus"
		                                metric.host = node_key
		                                metric.interval = collectd_interval
		                                metric.type = collectd_type
		                                metric.type_instance = stat_key
		                                metric.values = [int(status_metrics[key][node_key][stat_key])]
		                                metric.dispatch()
	collectd.info("HBase collection ended!")
# Init
collectd.register_init(restore_sigchld)
collectd.register_read(read_callback)
