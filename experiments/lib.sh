#!/usr/bin/env bash
# run_*.sh から source する共通関数。リポジトリのルートをカレントディレクトリとして使う。

BUILD_DIR="${BUILD_DIR:-build-release}"
RUNS="${RUNS:-3}"
EXTIME="${EXTIME:-5}"
TIMEOUT="${TIMEOUT:-600}"

bin_path() {
  echo "${BUILD_DIR}/cc/$1/${WORKLOAD}_$1.exe"
}

# prepare_exp <実験名>: バイナリの有無を確認し、results/<実験名>/meta.txt を書く。
prepare_exp() {
  EXP_DIR="results/$1"
  if [[ -e "${EXP_DIR}" ]]; then
    echo "error: ${EXP_DIR} は既に存在します。削除するか、EXPER で別の名前を指定してください。" >&2
    exit 1
  fi
  local cc
  for cc in "${PROTOCOLS[@]}"; do
    if [[ ! -x "$(bin_path "${cc}")" ]]; then
      echo "error: $(bin_path "${cc}") がありません。先にビルドしてください。" >&2
      exit 1
    fi
  done
  mkdir -p "${EXP_DIR}"
  write_meta "${EXP_DIR}/meta.txt"
}

write_meta() {
  local cache="${BUILD_DIR}/CMakeCache.txt"
  local build_type="unknown" compiler="unknown"
  if [[ -f "${cache}" ]]; then
    build_type="$(grep '^CMAKE_BUILD_TYPE:' "${cache}" | cut -d= -f2 || true)"
    compiler="$(grep '^CMAKE_CXX_COMPILER:' "${cache}" | cut -d= -f2 || true)"
    if [[ -x "${compiler}" ]]; then
      compiler="${compiler} ($("${compiler}" --version | head -n 1))"
    fi
  fi
  # results/ 自体は未追跡ファイルとして出るので、dirty の判定から外す。
  local dirty
  dirty="$(git status --short -- . ':!results' | sed 's/^/  /')"
  {
    echo "date:       $(date '+%Y-%m-%d %H:%M:%S')"
    echo "host:       $(hostname)"
    echo "branch:     $(git rev-parse --abbrev-ref HEAD)"
    echo "commit:     $(git rev-parse --short HEAD)"
    echo "build_dir:  ${BUILD_DIR}"
    echo "build_type: ${build_type}"
    echo "compiler:   ${compiler}"
    echo "nproc:      $(nproc 2>/dev/null || sysctl -n hw.ncpu)"
    echo "runs:       ${RUNS}"
    echo "extime:     ${EXTIME}"
    echo "load_start: $(uptime)"
    if [[ -n "${dirty}" ]]; then
      echo "dirty:      (下記の変更が未commit)"
      echo "${dirty}"
    else
      echo "dirty:      (なし)"
    fi
    echo "commands:"
  } > "$1"
}

finish_exp() {
  echo "load_end:   $(uptime)" >> "${EXP_DIR}/meta.txt"
}

# run_point <プロトコル> <条件ディレクトリ名> <ベンチマークの引数...>
run_point() {
  local cc="$1" cond="$2"
  shift 2
  local bin dir i status
  bin="$(bin_path "${cc}")"
  dir="${EXP_DIR}/${cc}/${cond}"
  mkdir -p "${dir}"
  echo "  ${bin} $* -extime=${EXTIME}" >> "${EXP_DIR}/meta.txt"
  for ((i = 1; i <= RUNS; i++)); do
    status=0
    if command -v timeout > /dev/null; then
      timeout "${TIMEOUT}" "${bin}" "$@" -extime="${EXTIME}" > "${dir}/run${i}.log" 2>&1 || status=$?
    else
      "${bin}" "$@" -extime="${EXTIME}" > "${dir}/run${i}.log" 2>&1 || status=$?
    fi
    if [[ ${status} -ne 0 ]]; then
      echo "${cc} ${cond} run${i}: FAILED (exit ${status}, 124 = timeout)"
    else
      echo "${cc} ${cond} run${i}: $(grep '^throughput\[tps\]' "${dir}/run${i}.log" | cut -f2) tps"
    fi
  done
}
