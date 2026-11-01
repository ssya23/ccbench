#pragma once

#include <atomic>
#include <source_location>

#include "../../../include/rwlock.hh"
#if LATCH_STATS
#include "../../../include/latch_stats.hh"
#endif

class LatchableRWLock : public ReaderWriteLock {
public:
  static constexpr int kLatched = -2;

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
