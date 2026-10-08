#pragma once

#include <atomic>

#include "../../../include/rwlock.hh"

class LatchableRWLock : public ReaderWriteLock {
public:
  static constexpr int kLatched = -2;

  int latch_lock() {
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
        return expected;
      }
    }
  }

  void latch_unlock(int new_value) {
    counter.store(new_value, std::memory_order_release);
  }
};
