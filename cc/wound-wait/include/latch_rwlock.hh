#pragma once

#include <source_location>

#include "../../../include/rwlock.hh"
#if LATCH_STATS
#include "../../../include/latch_stats.hh"
#endif

class LatchableRWLock : public ReaderWriteLock {
public:
  static constexpr int kLatched = -2;

  /**
   * この関数自体はlatch（排他）を取ることだけを目的としている。
   * Read/Writeロックが取れていることを保証しないので、
   * 呼び出し側でその後の操作（所有者確認・wound判断など）を明示する必要がある。
   * @return latchを取る直前のcounterの値（0=unlocked, >0=readロック数, -1=writeロック中）
   */
  int latch_lock([[maybe_unused]] std::source_location loc = std::source_location::current()) {
    int expected = counter.load(std::memory_order_acquire);
    for (;;) {
      if (expected == kLatched) {
        expected = counter.load(std::memory_order_acquire);
        _mm_pause();
        continue;
      }
      if (counter.compare_exchange_strong(
              expected, kLatched,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
#if LATCH_STATS
        latch_stats::on_acquired(loc.line(), loc.file_name());
#endif
        return expected;
      }
    }
  }

  void latch_unlock(int new_value) {
#if LATCH_STATS
    latch_stats::on_release();
#endif
    counter.store(new_value, std::memory_order_release);
  }
};