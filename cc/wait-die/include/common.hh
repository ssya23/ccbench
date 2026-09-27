#pragma once

#include <atomic>
#include <vector> 

#include "../../../include/cache_line_size.hh"
#include "../../../include/int64byte.hh"
#include "../../../include/masstree_wrapper.hh"
#include "tuple.hh"

#include "gflags/gflags.h"
#include "glog/logging.h"

#ifdef GLOBAL_VALUE_DEFINE
#define GLOBAL

#if MASSTREE_USE
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#endif

#else
#define GLOBAL extern

#if MASSTREE_USE
alignas(CACHE_LINE_SIZE) GLOBAL MasstreeWrapper<Tuple> MT;
#endif

#endif

#ifdef GLOBAL_VALUE_DEFINE
DEFINE_uint64(clocks_per_us, 2100,
              "CPU_MHz. Use this info for measuring time.");
DEFINE_uint64(extime, 3, "Execution time[sec].");
DEFINE_uint64(max_ope, 10,
              "Total number of operations per single transaction.");
DEFINE_bool(rmw, false,
            "True means read modify write, false means blind write.");
DEFINE_uint64(rratio, 50, "read ratio of single transaction.");
DEFINE_uint64(thread_num, 10, "Total number of worker threads.");
DEFINE_uint64(tuple_num, 1000000, "Total number of records.");
DEFINE_bool(ycsb, true,
            "True uses zipf_skew, false uses faster random generator.");
DEFINE_double(zipf_skew, 0, "zipf skew. 0 ~ 0.999...");
#else
DECLARE_uint64(clocks_per_us);
DECLARE_uint64(extime);
DECLARE_uint64(max_ope);
DECLARE_bool(rmw);
DECLARE_uint64(rratio);
DECLARE_uint64(thread_num);
DECLARE_uint64(tuple_num);
DECLARE_bool(ycsb);
DECLARE_double(zipf_skew);
#endif

class TxExecutor; //前方宣言を追加する必要
alignas(CACHE_LINE_SIZE) GLOBAL uint32_t TotalThreadNum;
GLOBAL std::vector<TxExecutor*> AllExecutors;

/* debug: どこでdieしたかをスレッドごとに数える. 全スレッド終了後に displayDieCounts() で合計を表示する. */
enum DieSite : uint32_t {
  DIE_READ_HEAD_FREE,     // read: waiterがいてheadより若い (lockは空いていた)
  DIE_READ_HEAD_HELD,     // read: waiterがいてheadより若い (readロック保持中)
  DIE_READ_OWNER,         // read: writeロックのownerが自分より古い
  DIE_UPGRADE_OWNER,      // update(upgrade): 自分より古いreaderがいる
  DIE_WRITE_OWNER,        // update(blind write): 自分より古いownerがいる
  DIE_DELETE_UPGRADE_OWNER, // delete(upgrade): 自分より古いreaderがいる
  DIE_DELETE_OWNER,       // delete: 自分より古いownerがいる
  DIE_SITE_NUM
};
struct alignas(CACHE_LINE_SIZE) DieCounter { uint64_t c[DIE_SITE_NUM] = {}; };
GLOBAL DieCounter DieCounts[64];
