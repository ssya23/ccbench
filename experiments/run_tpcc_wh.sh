#!/usr/bin/env bash
# TPC-C: スレッド数を固定し、warehouse 数を変えて各プロトコルを計測する。
# 使い方: experiments/run_tpcc_wh.sh
# 環境変数で変更可: EXPER THD PROTOCOLS WHS BUILD_DIR RUNS EXTIME TIMEOUT
set -euo pipefail
cd "$(dirname "$0")/.."
source experiments/lib.sh

WORKLOAD=tpcc
THD="${THD:-32}"
read -r -a PROTOCOLS <<< "${PROTOCOLS:-ss2pl wound-wait wound-wait2}"
read -r -a WHS <<< "${WHS:-1 2 4 8 16 24 32}"

prepare_exp "${EXPER:-tpcc_wh}"
# 時間帯による負荷の変化が特定のプロトコルに偏らないよう、プロトコルを内側のループにする。
for wh in "${WHS[@]}"; do
  for cc in "${PROTOCOLS[@]}"; do
    run_point "${cc}" "thd=${THD}_wh=${wh}" -thread_num="${THD}" -tpcc_num_wh="${wh}"
  done
done
finish_exp

python3 experiments/parse.py "${EXP_DIR}"
