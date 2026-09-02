ccbench_add_protocol(wound-wait
  SOURCES   transaction.cc util.cc
  WORKLOADS bomb tpcc ycsb
  OPTIONS
    DLR1
    KEY_SORT=${CCBENCH_KEY_SORT}
)
