#pragma once

#include <bit>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <vector>

#include "tsc.hh"

namespace latch_stats {

constexpr int kSubBits = 4;
constexpr int kSub = 1 << kSubBits;
constexpr int kBuckets = 1024;
constexpr int kMaxLine = 4096;
constexpr int kMaxSite = 96;

struct Site {
  std::uint32_t line = 0;
  const char* file = nullptr;
  std::uint64_t count = 0;
  std::uint64_t sum = 0;
  std::uint64_t max = 0;
  std::uint64_t hist[kBuckets] = {};
};

struct ThreadStats {
  std::int16_t site_of_line[kMaxLine];
  int nsite = 0;
  Site sites[kMaxSite];
  std::uint64_t start = 0;
  std::uint32_t line = 0;
  const char* file = nullptr;
  bool held = false;
  std::uint64_t nested = 0;
  std::uint64_t dropped = 0;

  ThreadStats() { std::memset(site_of_line, -1, sizeof(site_of_line)); }
};

inline std::mutex registry_mutex;
inline std::vector<ThreadStats*> registry;
inline thread_local ThreadStats* tls = nullptr;

inline ThreadStats& local() {
  if (tls == nullptr) {
    tls = new ThreadStats();
    std::lock_guard<std::mutex> guard(registry_mutex);
    registry.push_back(tls);
  }
  return *tls;
}

inline int bucket_of(std::uint64_t v) {
  if (v < static_cast<std::uint64_t>(kSub)) return static_cast<int>(v);
  const int e = static_cast<int>(std::bit_width(v)) - 1;
  const int sub = static_cast<int>((v >> (e - kSubBits)) & (kSub - 1));
  return (e - kSubBits + 1) * kSub + sub;
}

inline std::uint64_t bucket_lo(int b) {
  if (b < kSub) return static_cast<std::uint64_t>(b);
  const int e = b / kSub + kSubBits - 1;
  const std::uint64_t sub = static_cast<std::uint64_t>(b % kSub);
  return (static_cast<std::uint64_t>(kSub) + sub) << (e - kSubBits);
}

inline std::uint64_t bucket_hi(int b) {
  if (b < kSub) return static_cast<std::uint64_t>(b);
  const int e = b / kSub + kSubBits - 1;
  return bucket_lo(b) + (std::uint64_t{1} << (e - kSubBits)) - 1;
}

inline void on_acquired(std::uint32_t line, const char* file) {
  ThreadStats& t = local();
  if (t.held) ++t.nested;
  t.held = true;
  t.line = line;
  t.file = file;
  t.start = rdtsc();
}

inline void on_release() {
  const std::uint64_t end = rdtsc();
  ThreadStats* tp = tls;
  if (tp == nullptr || !tp->held) return;
  ThreadStats& t = *tp;
  t.held = false;
  const std::uint64_t d = end - t.start;
  if (t.line >= static_cast<std::uint32_t>(kMaxLine)) {
    ++t.dropped;
    return;
  }
  int idx = t.site_of_line[t.line];
  if (idx < 0) {
    if (t.nsite >= kMaxSite) {
      ++t.dropped;
      return;
    }
    idx = t.nsite++;
    t.site_of_line[t.line] = static_cast<std::int16_t>(idx);
    t.sites[idx].line = t.line;
    t.sites[idx].file = t.file;
  }
  Site& s = t.sites[idx];
  ++s.count;
  s.sum += d;
  if (d > s.max) s.max = d;
  ++s.hist[bucket_of(d)];
}

inline void merge(Site& dst, const Site& src) {
  dst.count += src.count;
  dst.sum += src.sum;
  if (src.max > dst.max) dst.max = src.max;
  for (int b = 0; b < kBuckets; ++b) dst.hist[b] += src.hist[b];
}

inline double quantile(const Site& s, double q) {
  if (s.count == 0) return 0.0;
  const std::uint64_t target = static_cast<std::uint64_t>(q * static_cast<double>(s.count - 1)) + 1;
  std::uint64_t acc = 0;
  for (int b = 0; b < kBuckets; ++b) {
    acc += s.hist[b];
    if (acc >= target) return (static_cast<double>(bucket_lo(b)) + static_cast<double>(bucket_hi(b))) / 2.0;
  }
  return static_cast<double>(s.max);
}

inline const char* basename_of(const char* path) {
  if (path == nullptr) return "?";
  const char* slash = std::strrchr(path, '/');
  return slash != nullptr ? slash + 1 : path;
}

inline void print_summary(const char* label, const Site& s, double clocks_per_us) {
  const double mean = s.count != 0 ? static_cast<double>(s.sum) / static_cast<double>(s.count) : 0.0;
  const double p50 = quantile(s, 0.50);
  const double p90 = quantile(s, 0.90);
  const double p99 = quantile(s, 0.99);
  std::printf(
      "[latch] site=%s\tcount=%llu\tmean_cycles=%.1f\tmedian_cycles=%.1f\tp90_cycles=%.1f\tp99_cycles=%.1f\tmax_cycles=%llu\tmean_us=%.4f\tmedian_us=%.4f\n",
      label, static_cast<unsigned long long>(s.count), mean, p50, p90, p99,
      static_cast<unsigned long long>(s.max), mean / clocks_per_us, p50 / clocks_per_us);
}

inline void print_hist(const char* label, const Site& s) {
  for (int b = 0; b < kBuckets; ++b) {
    if (s.hist[b] == 0) continue;
    std::printf("[latch_hist] site=%s\tlo_cycles=%llu\thi_cycles=%llu\tcount=%llu\n", label,
                static_cast<unsigned long long>(bucket_lo(b)),
                static_cast<unsigned long long>(bucket_hi(b)),
                static_cast<unsigned long long>(s.hist[b]));
  }
}

inline void dump(std::uint64_t clocks_per_us) {
  std::lock_guard<std::mutex> guard(registry_mutex);
  const double cpu = static_cast<double>(clocks_per_us);
  auto* all = new Site();
  auto* by_line = new std::vector<Site>(kMaxLine);
  std::uint64_t nested = 0;
  std::uint64_t dropped = 0;
  for (ThreadStats* t : registry) {
    nested += t->nested;
    dropped += t->dropped;
    for (int i = 0; i < t->nsite; ++i) {
      const Site& s = t->sites[i];
      Site& d = (*by_line)[s.line];
      d.line = s.line;
      if (d.file == nullptr) d.file = s.file;
      merge(d, s);
      merge(*all, s);
    }
  }
  std::printf("[latch] threads=%zu\tnested=%llu\tdropped=%llu\n", registry.size(),
              static_cast<unsigned long long>(nested), static_cast<unsigned long long>(dropped));
  print_summary("all", *all, cpu);
  char label[128];
  for (const Site& s : *by_line) {
    if (s.count == 0) continue;
    std::snprintf(label, sizeof(label), "%s:%u", basename_of(s.file), s.line);
    print_summary(label, s, cpu);
  }
  print_hist("all", *all);
  for (const Site& s : *by_line) {
    if (s.count == 0) continue;
    std::snprintf(label, sizeof(label), "%s:%u", basename_of(s.file), s.line);
    print_hist(label, s);
  }
  std::fflush(stdout);
  delete by_line;
  delete all;
}

}  // namespace latch_stats
