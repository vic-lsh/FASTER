v=8  # Value size
n=1000000000  # Number of objects
t=8  # Number of threads

num_op=1000000000  # Number of operations
num_warmup_op=1000000000  # Number of warmup operations

# Update value size
./change_value_size.sh $v

for value in {1..10}
do
  echo ------RUN $value---------
	d=3  # DRAM size
	FILE="pmem_benchmark_result_run_${value}.txt"

	# Start benchmark
	rm -rf $FILE
	touch $FILE
	numactl --cpunodebind 0 --membind 0 colorctl 0-$(($d * 8 - 1)) 0 ./pmem_benchmark 2 8 $t 0.9 $n $num_op $num_warmup_op | tee ${FILE}

	# Wait for benchmark to start
	while [ -z "$(cat $FILE | grep Running)" ]; do
		sleep 1
	done
	sleep 10

	# Measure perf counters for 5s
	sudo /root/yuhong/linux/tools/perf/perf record -e mem_load_retired.local_pmm:p, mem_load_retired.l3_miss:p -c 100 -o perf.data -p $(pgrep pmem_benchmark) sleep 5

	if [ -n "$(cat $FILE | grep 'ops/second/thread')" ]; then
		echo "Benchmark finish too soon. Please increase num_op and rerun."
		exit 1
	fi

	# Wait for benchmark to finish
	while [ -n "$(pgrep pmem_benchmark)" ]; do
		sleep 1
	done
    
  sudo cp perf.data perf_z_$value.txt
  sudo /root/yuhong/linux/tools/perf/perf script -F addr,phys_addr,time,event > z_3g_$value.txt
done
