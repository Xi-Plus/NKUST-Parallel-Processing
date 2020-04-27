start:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

stop:
	stop-dfs.sh
	stop-yarn.sh
	mr-jobhistory-daemon.sh stop historyserver
