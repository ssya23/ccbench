#pragma once

#include <atomic>
#include <unistd.h>
#include <sys/syscall.h>

#include "../../../include/rwlock.hh"

class LatchableRWLock : public ReaderWriteLock {
public:
  static constexpr int kLatched = -2;

  // デバッグ用: 最後にlatch_lock()に成功した/latch_unlock()した場所を記録する.
  // latch_unlock()を呼び忘れるバグを特定するための一時的な計測.
  std::atomic<void*> last_lock_site{nullptr};
  std::atomic<pid_t> last_lock_tid{0};
  std::atomic<void*> last_unlock_site{nullptr};
  std::atomic<pid_t> last_unlock_tid{0};

  /**
   * この関数自体はlatch（排他）を取ることだけを目的としている。
   * Read/Writeロックが取れていることを保証しないので、
   * 呼び出し側でその後の操作（所有者確認・wound判断など）を明示する必要がある。
   * @return latchを取る直前のcounterの値（0=unlocked, >0=readロック数, -1=writeロック中）
   */
  int latch_lock() {
    int expected = counter.load(std::memory_order_acquire);
    for (;;) {
      if (expected == kLatched) {
        expected = counter.load(std::memory_order_acquire);
        continue;
      }
      if (counter.compare_exchange_strong(
              expected, kLatched,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        last_lock_site.store(__builtin_return_address(0), std::memory_order_relaxed);
        last_lock_tid.store(static_cast<pid_t>(syscall(SYS_gettid)), std::memory_order_relaxed);
        return expected;
      }
    }
  }

  void latch_unlock(int new_value) {
    last_unlock_site.store(__builtin_return_address(0), std::memory_order_relaxed);
    last_unlock_tid.store(static_cast<pid_t>(syscall(SYS_gettid)), std::memory_order_relaxed);
    counter.store(new_value, std::memory_order_release);
  }
};
