set terminal png
set xlabel "throughput (txn/sec)"

# ARG1 is the csv file
# ARG2 is the output location of p50
# ARG3 is the output location of p95
# ARG4 is the output location of p99

set ylabel "p50 (ms)"
set title "latency throughput (p50)"
set output ARG2
set offset 1, 1, 1, 1
plot ARG1 using "ops/sec(cum)-newOrder":"p50(ms)" with linespoint,\
		 "" using "ops/sec(cum)-newOrder":"p50(ms)":"concurrency" with labels point pt 7 offset char 1, 1 notitle

#set ylabel "p95 (ms)"
#set title "latency throughput (p95)"
#set output ARG3
#set offset 1, 1, 1, 1
#plot ARG1 using "ops/sec(cum)-newOrder":"p95(ms)" with linespoint,\
#		 "" using "ops/sec(cum)-newOrder":"p95(ms)":"concurrency" with labels point pt 7offset char 1, 1 notitle

set ylabel "p99 (ms)"
set title "latency throughput (p99)"
set output ARG4
set offset 1, 1, 1, 1
plot ARG1 using "ops/sec(cum)-newOrder":"p99(ms)" with linespoint,\
		 "" using "ops/sec(cum)-newOrder":"p99(ms)":"concurrency" with labels point pt 7 offset char 1, 1 notitle
