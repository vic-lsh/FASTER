v=8  # Value size
n=1000000000  # Number of objects
d=12  # DRAM size
t=1  # Number of threads

num_op=100000000000  # Number of operations

# Update value size
./change_value_size.sh $v

for z in 0.7 0.9 0.99; do
    FILE="result_mm_thp_z_${z}_v_${v}_t_${t}_d_${d}_n_${n}.txt"
    rm -rf $FILE
    touch $FILE

    numactl --cpunodebind 0 --membind 0 colorctl 0-$(($d * 8 - 1)) 0 ./pmem_benchmark 2 8 $t $z $n $num_op > ${FILE}&
    while [ -z "$(cat $FILE | grep Running)" ]; do
        sleep 1
    done

    for i in {0..599}; do
        PERF_FILE="result_perf_mm_thp_z_${z}_v_${v}_t_${t}_d_${d}_n_${n}_i_${i}.txt"
        perf stat -C 0-$(($t - 1)) -e UNC_M_CAS_COUNT.ALL,UNC_M_PMM_CMD1.ALL,mem_load_retired.local_pmm,MEM_LOAD_RETIRED.L3_MISS,MEM_LOAD_RETIRED.L2_MISS,MEM_LOAD_RETIRED.L1_MISS -o $PERF_FILE sleep 1
    done

    kill $(pgrep pmem_benchmark)
done
