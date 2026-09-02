#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"
#include "include/timestamp.hh"

#include "../../include/cpu.hh"
#include "../../include/debug.hh"
#include "../../include/masstree_wrapper.hh"
#include "../../include/result.hh"
#include "../../include/tpcc.hh"
#include "../../include/tsc.hh"
#include "../../include/util.hh"

#include "../../common/runner.hh"

using namespace std;

int main(int argc, char* argv[]) try {
  gflags::SetUsageMessage("TPC-C SS2PL benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true); //コマンドライン引数の解析
  chkArg();
  TPCCWorkload<Tuple, void>::displayWorkloadParameter();
  TPCCWorkload<Tuple, void>::makeDB(nullptr); //ベンチマーク開始前のデータベース初期構築

  initResult(TotalThreadNum);

  ccbench::RunnerOptions opts;
  opts.display_per_tx = true;

// ccbench::run : 全プロトコル共通のスレッド起動・実行ループのテンプレート
  ccbench::run<TxExecutor, TransactionStatus, TPCCWorkload<Tuple, void>>(
      TotalThreadNum, opts,
      [](size_t thid, const bool& quit, Backoff& /*unused*/) {
        return TxExecutor(thid, &CCBenchResults[thid], quit);
      },
      [](TxExecutor& trans, size_t thid) {
        //ラムダ式=名前を付けずにその場で作る、小さな関数
        //[キャプチャ](引数) { 処理内容 }
        AllExecutors[thid] = &trans;
#ifdef Linux
        setThreadAffinity(thid);
#endif
#if MASSTREE_USE
        MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif
      });

  return 0;
} catch (const bad_alloc&) { ERR; }
