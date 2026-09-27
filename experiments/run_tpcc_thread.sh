#!/usr/bin/env bash
# TPC-C: warehouse 数を固定し、スレッド数を変えて各プロトコルを計測する。
# 使い方: experiments/run_tpcc_thread.sh
# 環境変数で変更可: EXPER WH PROTOCOLS THREADS BUILD_DIR RUNS EXTIME TIMEOUT
set -euo pipefail
cd "$(dirname "$0")/.."
source experiments/lib.sh

WORKLOAD=tpcc
WH="${WH:-1}"
read -r -a PROTOCOLS <<< "${PROTOCOLS:-ss2pl wound-wait wound-wait2}"
read -r -a THREADS <<< "${THREADS:-1 2 4 8 16 32 48 64}"

prepare_exp "${EXPER:-tpcc_thread}"
# 時間帯による負荷の変化が特定のプロトコルに偏らないよう、プロトコルを内側のループにする。
for thd in "${THREADS[@]}"; do
  for cc in "${PROTOCOLS[@]}"; do
    run_point "${cc}" "thd=${thd}_wh=${WH}" -thread_num="${thd}" -tpcc_num_wh="${WH}"
  done
done
finish_exp

python3 experiments/parse.py "${EXP_DIR}"
