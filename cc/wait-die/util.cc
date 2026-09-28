
#include <stdlib.h>
#include <sys/syscall.h> // syscall(SYS_gettid),
#include <sys/types.h>   // syscall(SSY_gettid),
#include <unistd.h>      // syscall(SSY_gettid),

#include <atomic>
#include <bitset>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <thread>
#include <type_traits>
#include <vector>

#include "../../include/config.hh"
#include "../../include/debug.hh"
#include "../../include/masstree_wrapper.hh"
#include "../../include/procedure.hh"
#include "../../include/random.hh"
#include "../../include/result.hh"
#include "../../include/zipf.hh"
#include "include/common.hh"
#include "include/tuple.hh"
#include "include/util.hh"

void chkArg() {
  displayParameter();

  if (FLAGS_rratio > 100) { ERR; }

  TotalThreadNum = FLAGS_thread_num;
  if (TotalThreadNum > 64) {
    cout << "thread_num must be <= 64 (Tuple::owners_bitmap is a 64-bit word)" << endl;
    ERR;
  }
  AllExecutors.resize(TotalThreadNum, nullptr); 

  if (FLAGS_clocks_per_us < 100) {
    cout << "CPU_MHZ is less than 100. are your really?" << endl;
    ERR;
  }
}

void displayDB() {}

void displayDieCounts() {
  static const char* names[DIE_SITE_NUM] = {
      "read_head_free", "read_head_held", "read_owner", "upgrade_owner",
      "write_owner", "delete_upgrade_owner", "delete_owner"};
  uint64_t total[DIE_SITE_NUM] = {};
  for (uint32_t t = 0; t < TotalThreadNum; ++t)
    for (uint32_t i = 0; i < DIE_SITE_NUM; ++i) total[i] += DieCounts[t].c[i];
  cout << "Die counts by site:" << endl;
  for (uint32_t i = 0; i < DIE_SITE_NUM; ++i)
    cout << "  die_" << names[i] << ":\t" << total[i] << endl;
}

void displayWaitCounts() {
  static const char* names[WAIT_KIND_NUM] = {"read", "write", "upgrade"};
  uint64_t cnt[WAIT_KIND_NUM] = {}, cycles[WAIT_KIND_NUM] = {}, max_cycles[WAIT_KIND_NUM] = {};
  uint64_t nonhead[WAIT_KIND_NUM] = {}, max_nonhead[WAIT_KIND_NUM] = {};
  uint64_t head[WAIT_KIND_NUM] = {}, max_head[WAIT_KIND_NUM] = {}, head_lost[WAIT_KIND_NUM] = {};
  uint64_t checks[WAIT_KIND_NUM] = {}, mismatch[WAIT_KIND_NUM] = {}, max_ahead[WAIT_KIND_NUM] = {};
  for (uint32_t t = 0; t < TotalThreadNum; ++t) {
    const WaitCounter& w = WaitCounts[t];
    for (uint32_t i = 0; i < WAIT_KIND_NUM; ++i) {
      cnt[i] += w.cnt[i];
      cycles[i] += w.cycles[i];
      if (w.max_cycles[i] > max_cycles[i]) max_cycles[i] = w.max_cycles[i];
      nonhead[i] += w.nonhead_cycles[i];
      if (w.max_nonhead_cycles[i] > max_nonhead[i]) max_nonhead[i] = w.max_nonhead_cycles[i];
      head[i] += w.head_cycles[i];
      if (w.max_head_cycles[i] > max_head[i]) max_head[i] = w.max_head_cycles[i];
      head_lost[i] += w.head_lost[i];
      checks[i] += w.checks[i];
      mismatch[i] += w.mismatch[i];
      if (w.max_ahead[i] > max_ahead[i]) max_ahead[i] = w.max_ahead[i];
    }
  }
  const double cpu = static_cast<double>(FLAGS_clocks_per_us);
  cout << "Wait counts (time in us, clocks_per_us=" << FLAGS_clocks_per_us << "):" << endl;
  for (uint32_t i = 0; i < WAIT_KIND_NUM; ++i) {
    cout << "  wait_" << names[i] << "_count:\t" << cnt[i] << endl;
    cout << "  wait_" << names[i] << "_total_us:\t" << cycles[i] / cpu << endl;
    cout << "  wait_" << names[i] << "_avg_us:\t" << (cnt[i] ? cycles[i] / cpu / cnt[i] : 0) << endl;
    cout << "  wait_" << names[i] << "_max_us:\t" << max_cycles[i] / cpu << endl;
    cout << "  wait_" << names[i] << "_nonhead_total_us:\t" << nonhead[i] / cpu << endl;
    cout << "  wait_" << names[i] << "_nonhead_max_us:\t" << max_nonhead[i] / cpu << endl;
    cout << "  wait_" << names[i] << "_head_total_us:\t" << head[i] / cpu << endl;
    cout << "  wait_" << names[i] << "_head_max_us:\t" << max_head[i] / cpu << endl;
    cout << "  wait_" << names[i] << "_head_lost_count:\t" << head_lost[i] << endl;
    cout << "  wait_" << names[i] << "_position_checks:\t" << checks[i] << endl;
    cout << "  wait_" << names[i] << "_head_flag_mismatch:\t" << mismatch[i] << endl;
    cout << "  wait_" << names[i] << "_max_ahead:\t" << max_ahead[i] << endl;
  }
}

void displayParameter() {
  cout << "#FLAGS_clocks_per_us:\t" << FLAGS_clocks_per_us << endl;
  cout << "#FLAGS_extime:\t\t" << FLAGS_extime << endl;
  cout << "#FLAGS_max_ope:\t\t" << FLAGS_max_ope << endl;
  cout << "#FLAGS_rmw:\t\t" << FLAGS_rmw << endl;
  cout << "#FLAGS_rratio:\t\t" << FLAGS_rratio << endl;
  cout << "#FLAGS_thread_num:\t" << FLAGS_thread_num << endl;
  cout << "#FLAGS_tuple_num:\t" << FLAGS_tuple_num << endl;
  cout << "#FLAGS_ycsb:\t\t" << FLAGS_ycsb << endl;
  cout << "#FLAGS_zipf_skew:\t" << FLAGS_zipf_skew << endl;
}

void partTableInit([[maybe_unused]] size_t thid,
                   [[maybe_unused]] uint64_t start,
                   [[maybe_unused]] uint64_t end) {}

void makeDB() {}

void ShowOptParameters() {
  cout << "#ShowOptParameters()"
       << ": ADD_ANALYSIS " << ADD_ANALYSIS << ": BACK_OFF " << BACK_OFF
#ifdef DLR0
       << ": DLR0 "
#elif defined DLR1
       << ": DLR1 "
#endif
       << ": MASSTREE_USE " << MASSTREE_USE << ": KEY_SIZE " << KEY_SIZE
       << ": KEY_SORT " << KEY_SORT << ": VAL_SIZE " << VAL_SIZE << endl;
}
