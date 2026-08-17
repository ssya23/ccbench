#pragma once

#include <atomic>
#include <cstdint>

#include "../../../include/cache_line_size.hh"

#ifdef GLOBAL_VALUE_DEFINE
#define GLOBAL
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<uint32_t> TimestampCounter(0);
#else
#define GLOBAL extern
alignas(CACHE_LINE_SIZE) GLOBAL std::atomic<uint32_t> TimestampCounter;
#endif