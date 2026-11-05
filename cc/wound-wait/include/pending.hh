#pragma once

#include <atomic>

constexpr int kMaxPending = 64;

struct Pending {
  int n = 0;
  std::atomic<int>* target[kMaxPending];
  int delta[kMaxPending];
};

inline thread_local Pending pending;
