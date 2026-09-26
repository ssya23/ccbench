#pragma once

#include <vector>
#include <queue>
#include <deque>
#include "../../../include/tx_executor_concept.hh"

#include "../../../include/backoff.hh"
#include "../../../include/procedure.hh"
#include "../../../include/result.hh"
#include "../../../include/string.hh"
#include "../../../include/util.hh"
#include "ss2pl_op_element.hh"
#include "tuple.hh"
#include "waitentry.hh"
#include "latch_rwlock.hh"

enum class TransactionStatus : uint8_t {
  invalid,
  inflight,
  committed,
  aborted,
};

enum class LockResult {
    SUCCESS,
    ABORTED,
    NOT_FOUND, 
    FAILED
};

extern void writeValGenerator(char* writeVal, size_t val_size, size_t thid);

class TxExecutor {
public:
  alignas(CACHE_LINE_SIZE) int thid_;
  int local_timestamp;
  Result* result_;
  Backoff backoff_;
  alignas(CACHE_LINE_SIZE)
  std::deque<SetElement<Tuple>> read_set_;
  std::deque<SetElement<Tuple>> write_set_;
  vector<Procedure> pro_set_;
  std::deque<Tuple*> gc_records_;
  std::vector<ReaderWriteLock*> r_lock_list_;
  std::vector<ReaderWriteLock*> w_lock_list_;
  const bool& quit_; // for thread termination control
  bool reconnoitering_ = false;
  bool is_ronly_ = false;
  bool is_batch_ = false;

  alignas(CACHE_LINE_SIZE) std::atomic<TransactionStatus> status_ = TransactionStatus::inflight;
  alignas(CACHE_LINE_SIZE) std::atomic<int> waiter_count_ = 0; // 自分が保持しているタプルのうち、待ち行列が空でないものの数
  alignas(CACHE_LINE_SIZE) WaitEntry wait_entry;

  TxExecutor(int thid, Result* res, const bool& quit)
      : thid_(thid), result_(res), backoff_(FLAGS_clocks_per_us), quit_(quit) {
  }

  SetElement<Tuple>* searchReadSet(Storage s, std::string_view key);

  SetElement<Tuple>* searchWriteSet(Storage s, std::string_view key);

  void begin();

  void read(uint64_t key);
  Status read(Storage s, std::string_view key, TupleBody** body); 
  LockResult  read_internal(Storage s, std::string_view key, Tuple* tuple, int rcounter);

  Status scan(Storage s, std::string_view left_key, bool l_exclusive,
              std::string_view right_key, bool r_exclusive,
              std::vector<TupleBody*>& result);

  Status scan(Storage s, std::string_view left_key, bool l_exclusive,
              std::string_view right_key, bool r_exclusive,
              std::vector<TupleBody*>& result, int64_t limit);

  void write(uint64_t key);
  Status update(Storage s, std::string_view key, TupleBody&& body);

  void readWrite(uint64_t key);

  Status insert(Storage s, std::string_view key, TupleBody&& body);

  Status delete_record(Storage s, std::string_view key);

  bool commit();

  void abort();

  Status read_lock(Storage s, std::string_view key);
  Status write_lock(Storage s, std::string_view key);

  void reconnoiter_begin();
  void reconnoiter_end();

  bool isLeader();

  void leaderWork();
  // inline
  Tuple* get_tuple(Tuple* table, uint64_t key) { return &table[key]; }

  /* wound-wait用の関数を追加 */  
  LockResult wait_readop(Tuple* tuple, Storage s);

  LockResult wait_writeop(Tuple* tuple, Storage s);

  LockResult wait_upgradeop(Tuple* tuple, Storage s);

  LockResult wound_writelock(Tuple *tuple, Storage s);

  int wound_readlock(Tuple *tuple, int counter, Storage s);
};

static_assert(TxExecutorLike<TxExecutor>);
