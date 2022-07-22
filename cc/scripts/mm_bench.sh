v=8  # Value size
n=1000000000  # Number of objects
t=8  # Number of threads

num_op=1000000000  # Number of operations
num_warmup_op=1000000000  # Number of warmup operations

# Update value size
./change_value_size.sh $v

for z in 0.7 0.9 0.99; do
	for dd in {5..28}; do
		d=$((2 * $dd))  # DRAM size
		FILE="result_mm_thp_z_${z}_v_${v}_t_${t}_d_${d}_n_${n}.txt"
		PERF_FILE="result_perf_mm_thp_z_${z}_v_${v}_t_${t}_d_${d}_n_${n}.txt"

		# Start benchmark
		rm -rf $FILE
		touch $FILE
		numactl --cpunodebind 0 --membind 0 colorctl 0-$(($d * 8 - 1)) 0 ./pmem_benchmark 2 8 $t $z $n $num_op $num_warmup_op > ${FILE}&

		# Wait for benchmark to start
		while [ -z "$(cat $FILE | grep Running)" ]; do
			sleep 1
		done
		sleep 10

		# Measure perf counters for 5s
		perf stat -C 0-$(($t - 1)) -e UNC_M_CAS_COUNT.ALL,UNC_M_PMM_CMD1.ALL,mem_load_retired.local_pmm,MEM_LOAD_RETIRED.L3_MISS,MEM_LOAD_RETIRED.L2_MISS,MEM_LOAD_RETIRED.L1_MISS -o $PERF_FILE sleep 5
		if [ -n "$(cat $FILE | grep 'ops/second/thread')" ]; then
			echo "Benchmark finish too soon. Please increase num_op and rerun."
			exit 1
		fi

		# Wait for benchmark to finish
		while [ -n "$(pgrep pmem_benchmark)" ]; do
			sleep 1
		done
	done
done
