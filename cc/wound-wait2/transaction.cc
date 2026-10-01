
#include <stdio.h>
#include <string.h>
#include <cstdarg>

#include <atomic>
#include <bit>
#include <xmmintrin.h>

#include "../../include/backoff.hh"
#include "../../include/debug.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "include/common.hh"
#include "include/transaction.hh"
#include "include/timestamp.hh"

using namespace std;

inline SetElement<Tuple>* TxExecutor::searchReadSet(Storage s,
                                                    std::string_view key) {
  for (auto& re : read_set_) {
    if (re.storage_ != s) continue;
    if (re.key_ == key) return &re;
  }

  return nullptr;
}

inline SetElement<Tuple>* TxExecutor::searchWriteSet(Storage s,
                                                     std::string_view key) {
  for (auto& we : write_set_) {
    if (we.storage_ != s) continue;
    if (we.key_ == key) return &we;
  }

  return nullptr;
}

/**
 * @brief function about abort.
 * Clean-up local read/write set.
 * Release locks.
 * @return void
 */
void TxExecutor::abort() {

  /* Release locks 
   * abortされたTXは他のthreadに Lock&owners を解放されている可能性がある. よって,latch取得後以下の二つの挙動のどちらかを取る
   * 1.Lock&ownersがまだ残されている場合, 解放する.
   * 2.Lock&ownersがすでに他のthreadによって解放されているのでskip */

	for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
		Tuple* tuple = (*itr).rcdptr_;
	  int prev = tuple->lock_.latch_lock();

    /* ReadSetのループでは,owners_bitmapを確認してロックをまだ保持していることを確認. 保持していれば,ロック(counter)を解放する. 
     * counterをチェックして counter=-1 ならskipする. Upgradeが発生していることを意味し,WriteSetのループで解放する. */
      
		if(tuple->has_owner(thid_)){ 
			if(prev != -1){               
        tuple->del_owner(thid_);
				prev -= 1;
			}
		}
		tuple->lock_.latch_unlock(prev);
	}

	for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
		  Tuple* tuple = (*itr).rcdptr_;
		  int prev = tuple->lock_.latch_lock();

      if ((*itr).op_ == OpType::INSERT) {
        Masstrees[get_storage((*itr).storage_)].remove_value_if_present((*itr).key_);
        tuple->delete_flag = true;
        gc_records_.push_back(tuple);
      }

		  if(tuple->has_owner(thid_)){
				tuple->del_owner(thid_);
				prev = 0;
			}
		  tuple->lock_.latch_unlock(prev);
	}

  /* Clean-up local read/write set.*/
  read_set_.clear();
  write_set_.clear();

#if BACK_OFF
#if ADD_ANALYSIS
  uint64_t start(rdtscp());
#endif

  Backoff::backoff(FLAGS_clocks_per_us);

#if ADD_ANALYSIS
  result_->local_backoff_latency_ += rdtscp() - start;
#endif

#endif
}

/**
 * @brief success termination of transaction.
 * @return void
 */
bool TxExecutor::commit() {

	TransactionStatus expected = TransactionStatus::inflight;
	bool success = status_.compare_exchange_strong(expected, TransactionStatus::committed, memory_order_acq_rel, memory_order_acquire);
	if (!success) return false; //tpcc.hh or ycsb.hh内でcommit()の戻り値がfalseならabort()が実行される. 

  /* 取得したLock&owners_bitmapは全てこのthreadが解放する */

	for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
		  Tuple* tuple = (*itr).rcdptr_;

		  int prev = tuple->lock_.latch_lock();

		  if(prev != -1){
			  	tuple->del_owner(thid_);
				  prev -= 1;
		  }
		  tuple->lock_.latch_unlock(prev);
	}


	for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
		  Tuple* tuple = (*itr).rcdptr_;

		  int prev = tuple->lock_.latch_lock();

		  switch ((*itr).op_) {
		    case OpType::UPDATE: {
		      memcpy((*itr).rcdptr_->body_.get_val_ptr(), (*itr).body_.get_val_ptr(),
		             (*itr).body_.get_val_size());
		      break;
		    }
		    case OpType::INSERT: {
          tuple->committed_record = true;
		      break;
		    }
		    case OpType::DELETE: {
		      Masstrees[get_storage((*itr).storage_)].remove_value_if_present((*itr).key_);
          tuple->delete_flag = true;
		      gc_records_.push_back((*itr).rcdptr_);
		      break;
		    }
		    default:
		      ERR;
		  }

      tuple->del_owner(thid_);
      prev = 0;
      tuple->lock_.latch_unlock(prev);
    }

  /* Clean-up local read/write set */
  read_set_.clear();
  write_set_.clear();
  return true;
}

/**
 * @brief Initialize function of transaction.
 * Allocate timestamp.
 * @return void */
void TxExecutor::begin() {
  bool is_retry = (this->status_ == TransactionStatus::aborted);
  this->status_ = TransactionStatus::inflight;
  this->waiter_count_.store(0, std::memory_order_relaxed);
  if (!is_retry) {
    this->local_timestamp = TimestampCounter.fetch_add(1, std::memory_order_relaxed);
  }
}

/**
 * @brief Transaction read function.
 * @param [in] key The key of key-value
 */
Status TxExecutor::read(Storage s, std::string_view key, TupleBody** body) {
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif // ADD_ANALYSIS
  SetElement<Tuple>* e;

  /* read-own-writes or re-read from local read set. */
  e = searchReadSet(s, key);
  if (e) {
    *body = &(e->body_);
    goto FINISH_READ;
  }
  e = searchWriteSet(s, key);
  if (e) {
    *body = &(e->body_);
    goto FINISH_READ;
  }

  /* Search tuple from data structure. */
  Tuple* tuple;
  tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
  ++result_->local_tree_traversal_;
#endif
  if (tuple == nullptr) return Status::WARN_NOT_FOUND;

  int rcounter;
  rcounter = tuple->lock_.latch_lock();

  if(tuple->delete_flag == true){
    tuple->lock_.latch_unlock(rcounter);
    return Status::WARN_NOT_FOUND;
  }

  LockResult readresult;
  readresult = read_internal(s, key, tuple, rcounter);

  if(readresult == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;
  if(readresult == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;

  *body = &(read_set_.back().body_); 

FINISH_READ:
#if ADD_ANALYSIS
  result_->local_read_latency_ += rdtscp() - start;
#endif
  return Status::OK;
}

static inline void increment_waitcount(Tuple* tuple) {
  for (uint64_t bits = tuple->owners_bitmap; bits != 0; bits &= bits - 1)
    AllExecutors[std::countr_zero(bits)]->waiter_count_.fetch_add(1, std::memory_order_acq_rel);
}

static inline void decrement_waitcount(Tuple* tuple) {
  for (uint64_t bits = tuple->owners_bitmap; bits != 0; bits &= bits - 1)
    AllExecutors[std::countr_zero(bits)]->waiter_count_.fetch_sub(1, std::memory_order_acq_rel);
}

static inline bool has_younger_owner(Tuple* tuple, int thid, int ts) {
  for (uint64_t bits = tuple->owners_bitmap; bits != 0; bits &= bits - 1) {
    const int i = std::countr_zero(bits);
    if (i == thid) continue;
    if (AllExecutors[i]->local_timestamp > ts) return true;
  }
  return false;
}

static inline bool has_older_waiter(Tuple* tuple, int ts) {
  return tuple->waiters_head != nullptr && tuple->waiters_head->ts < ts;
}

LockResult TxExecutor::read_internal(Storage s, std::string_view key, Tuple* tuple, int rcounter) {
  TupleBody body;

  if (reconnoitering_) goto FINISH_READ_LOCK;

  if (!has_older_waiter(tuple, this->local_timestamp)) {
    bool waited = this->waiter_count_.load() > 0 || tuple->waiters_head != nullptr;

    if (rcounter == -1) {
      if (waited) {
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED) { tuple->lock_.latch_unlock(rcounter); return LockResult::ABORTED; }
        else if (woundresult == LockResult::NOT_FOUND) { tuple->lock_.latch_unlock(0); return LockResult::NOT_FOUND; }
        else if (woundresult == LockResult::SUCCESS) rcounter = 0;
      }
    }

    if (rcounter >= 0) {
      tuple->add_owner(thid_);
      rcounter++;
      if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
      tuple->lock_.latch_unlock(rcounter);
      goto FINISH_READ_LOCK;
    }

    if (tuple->waiters_head == nullptr) increment_waitcount(tuple);
  }

  this->wait_entry.insertInto(tuple, this->local_timestamp);
  tuple->lock_.latch_unlock(rcounter);

  LockResult result;
  result = wait_readop(tuple);
  if(result == LockResult::SUCCESS) goto FINISH_READ_LOCK;
  if(result == LockResult::ABORTED) return LockResult::ABORTED;
  if(result == LockResult::NOT_FOUND) return LockResult::NOT_FOUND;

FINISH_READ_LOCK:
  body = TupleBody(tuple->body_.get_key(), tuple->body_.get_val(),
                   tuple->body_.get_val_align());
  read_set_.emplace_back(s, key, tuple, std::move(body));

  return  LockResult::SUCCESS;
}

Status TxExecutor::scan(const Storage s, std::string_view left_key,
                        bool l_exclusive, std::string_view right_key,
                        bool r_exclusive, std::vector<TupleBody*>& result) {
  return scan(s, left_key, l_exclusive, right_key, r_exclusive, result, -1);// これを指定すると範囲内のrecordを全部取得する.
}

Status TxExecutor::scan(const Storage s, std::string_view left_key,
                        bool l_exclusive, std::string_view right_key,
                        bool r_exclusive, std::vector<TupleBody*>& result,
                        int64_t limit) {
  result.clear();

  std::vector<Tuple*> scan_res;
  Masstrees[get_storage(s)].scan(
      left_key.empty() ? nullptr : left_key.data(), left_key.size(),
      l_exclusive, right_key.empty() ? nullptr : right_key.data(),
      right_key.size(), r_exclusive, &scan_res, limit);

  for (auto&& itr : scan_res) {

    //既にこのトランザクションが読んだことがある
    SetElement<Tuple>* e = searchReadSet(s, itr->body_.get_key());
    if (e) {
      result.emplace_back(&(e->body_));
      continue;
    }

    //既にこのトランザクションが書き込んだことがあるか
    e = searchWriteSet(s, itr->body_.get_key());
    if (e) {
      result.emplace_back(&(e->body_));
      continue;
    }

    int rcounter = itr->lock_.latch_lock();
    if(itr->delete_flag == true){
      itr->lock_.latch_unlock(rcounter);
      continue;
    }

    LockResult readresult = read_internal(s, itr->body_.get_key(), itr, rcounter);
    if(readresult == LockResult::NOT_FOUND) continue;
    if (readresult == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
    result.emplace_back(&(read_set_.back().body_));
  }

  return Status::OK;
}

/**
 * @brief transaction write operation
 * @param [in] key The key of key-value
 * @return void
 */
Status TxExecutor::update(Storage s, std::string_view key, TupleBody&& body) {
#if ADD_ANALYSIS
  uint64_t start = rdtscp();
#endif

  // if it already wrote the key object once.
  if (searchWriteSet(s, key)) goto FINISH_WRITE;

  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr) {

    if ((*rItr).storage_ != s) continue;
    if ((*rItr).key_ == key) { // hit

      int upcounter = (*rItr).rcdptr_->lock_.latch_lock();
      
      if((*rItr).rcdptr_->delete_flag == true){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::WARN_NOT_FOUND;
      }

      /* このrecordのReadLockは自分が持っているはずだが、woundされている場合はwound_readlock()が他スレッドから既に解放している(ownersを落としcounterを減らす)
       * よってstatusを見ないと、upcounter==1が自分によるものだとは断定できない。
       *
       * 確認後にwoundされても問題ない。status_はlatch外から変えられるが、counterとownersの変更にはlatchが要るので、このlatch区間では自分のReadLockは残る*/
      TransactionStatus ts = status_.load(std::memory_order_acquire);
      if(ts == TransactionStatus::aborted){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::ERROR_LOCK_FAILED;
      }

      Tuple* utuple = (*rItr).rcdptr_;

      if (!has_older_waiter(utuple, this->local_timestamp)) {
        bool waited = this->waiter_count_.load() > 0 || utuple->waiters_head != nullptr;

        if (waited) {
          if (upcounter > 1) {
            upcounter = wound_readlock(utuple, upcounter);

            if(this->status_.load(std::memory_order_acquire) == TransactionStatus::aborted){utuple->lock_.latch_unlock(upcounter); return Status::ERROR_LOCK_FAILED;}
          }
        }

        // Lock取得可能. Upgradeできる
        if(upcounter == 1){
          upcounter = -1;
          utuple->add_owner(thid_);
          utuple->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, utuple, std::move(body),OpType::UPDATE);
          goto FINISH_WRITE;
        }

        if (utuple->waiters_head == nullptr) increment_waitcount(utuple);
      }

      this->wait_entry.insertInto(utuple, local_timestamp);

      utuple->lock_.latch_unlock(upcounter);

      LockResult result = wait_upgradeop(utuple);
      if (result == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
      if(result == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;

      write_set_.emplace_back(s, key, utuple, std::move(body),OpType::UPDATE);

      goto FINISH_WRITE;
    }
  }


  /**
   * Search tuple from data structure.
   */
  Tuple* tuple;
  tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
  ++result_->local_tree_traversal_;
#endif

  if (tuple == nullptr) return Status::WARN_NOT_FOUND;

  int wcounter;
  wcounter = tuple->lock_.latch_lock();

  if(tuple->delete_flag == true){
    tuple->lock_.latch_unlock(wcounter);
    return Status::WARN_NOT_FOUND;
  }

  bool acquired;
  acquired = false;

  if (!has_older_waiter(tuple, this->local_timestamp)) {
    bool waited = this->waiter_count_.load() > 0 || tuple->waiters_head != nullptr;

    if (waited) {
      if (wcounter == -1){
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED) { tuple->lock_.latch_unlock(wcounter); return Status::ERROR_LOCK_FAILED; }
        else if (woundresult == LockResult::NOT_FOUND) { tuple->lock_.latch_unlock(0); return Status::WARN_NOT_FOUND; }
        else if (woundresult == LockResult::SUCCESS) wcounter = 0;

      }else if(wcounter >= 1) {
        wcounter = wound_readlock(tuple, wcounter);
        if (this->status_.load(std::memory_order_acquire) == TransactionStatus::aborted) { tuple->lock_.latch_unlock(wcounter); return Status::ERROR_LOCK_FAILED; }
      }
    }

    if(wcounter == 0){
      tuple->add_owner(thid_);
      wcounter = -1;
      acquired = true;
      if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
    }else{
      if (tuple->waiters_head == nullptr) increment_waitcount(tuple);
      this->wait_entry.insertInto(tuple, local_timestamp);
    }

  }else this->wait_entry.insertInto(tuple, local_timestamp);

  tuple->lock_.latch_unlock(wcounter);

  if (!acquired) {
    LockResult result = wait_writeop(tuple);
    if (result == LockResult::ABORTED)return Status::ERROR_LOCK_FAILED;
    if (result == LockResult::NOT_FOUND)return Status::WARN_NOT_FOUND;
  }

  this->write_set_.emplace_back(s, key, tuple, std::move(body), OpType::UPDATE);

FINISH_WRITE:
#if ADD_ANALYSIS
  result_->local_write_latency_ += rdtscp() - start;
#endif

return Status::OK;
}

Status TxExecutor::insert(Storage s, std::string_view key, TupleBody&& body) {
#if ADD_ANALYSIS
  std::uint64_t start = rdtscp();
#endif

  if (searchWriteSet(s, key)) return Status::WARN_ALREADY_EXISTS;

  Tuple* tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
  ++result_->local_tree_traversal_;
#endif
  if (tuple != nullptr) { return Status::WARN_ALREADY_EXISTS; }

  tuple = new Tuple();
  tuple->init(std::move(body));
  tuple->add_owner(this->thid_);
  // delete_flag/committed_recordはデフォルトのfalseのまま.
  // commit()でcommitted_record=trueにする. wound_writelockがcommitted_record==falseのままこの行をwoundした場合はdelete_flag=trueにする.

  Status stat = Masstrees[get_storage(s)].insert_value(key, tuple);
  if (stat == Status::WARN_ALREADY_EXISTS) {
    delete tuple;
    return stat;
  }

  write_set_.emplace_back(s, key, tuple, OpType::INSERT);

#if ADD_ANALYSIS
  result_->local_write_latency_ += rdtscp() - start;
#endif
  return Status::OK;
}

Status TxExecutor::delete_record(Storage s, std::string_view key) {
#if ADD_ANALYSIS
  std::uint64_t start = rdtscp();
#endif

  // cancel previous write
  for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr) {
    if ((*itr).storage_ != s) continue;
    if ((*itr).key_ == key){
      (*itr).op_ = OpType::DELETE;
      goto FINISH_DELETE;
    }
  }

  for (auto rItr = read_set_.begin(); rItr != read_set_.end(); ++rItr) {
    if ((*rItr).storage_ != s) continue;
    if ((*rItr).key_ == key) { // hit
      int upcounter = (*rItr).rcdptr_->lock_.latch_lock();

      if((*rItr).rcdptr_->delete_flag == true){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::WARN_NOT_FOUND;
      }

      TransactionStatus ts = status_.load(std::memory_order_acquire);
      if(ts == TransactionStatus::aborted){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::ERROR_LOCK_FAILED;
      }

      Tuple* utuple = (*rItr).rcdptr_;

      if (!has_older_waiter(utuple, this->local_timestamp)) {
        bool waited = this->waiter_count_.load() > 0 || utuple->waiters_head != nullptr;

        if (waited) {
          if (upcounter > 1) {
            upcounter = wound_readlock(utuple, upcounter);
            if(this->status_.load(std::memory_order_acquire) == TransactionStatus::aborted){
              utuple->lock_.latch_unlock(upcounter);
              return Status::ERROR_LOCK_FAILED;
            }
          }
        }

        // Lock取得可能. Upgradeできる
        if(upcounter == 1){
          upcounter = -1;
          utuple->add_owner(thid_);
          utuple->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, utuple, OpType::DELETE);
          goto FINISH_DELETE;
        }

        if (utuple->waiters_head == nullptr) increment_waitcount(utuple);
      }

      this->wait_entry.insertInto(utuple, local_timestamp);

      utuple->lock_.latch_unlock(upcounter);

      LockResult result = wait_upgradeop(utuple);
      if(result == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
      if(result == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;
      //ReadLockを取得していたのにdeleteされるということは,すでに自分がabortされていることを意味する.

      write_set_.emplace_back(s, key, utuple, OpType::DELETE);
      goto FINISH_DELETE;
    }
  }

  /*　Search tuple from data structure */
  Tuple* tuple;
  tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
  ++result_->local_tree_traversal_;
#endif
  if (tuple == nullptr)return Status::WARN_NOT_FOUND;

  int wcounter;
  wcounter = tuple->lock_.latch_lock();

  if(tuple->delete_flag == true){
    tuple->lock_.latch_unlock(wcounter);
    return Status::WARN_NOT_FOUND;
  }

  bool acquired;
  acquired = false;

  if (!has_older_waiter(tuple, this->local_timestamp)) {
    bool waited = this->waiter_count_.load() > 0 || tuple->waiters_head != nullptr;

    if (waited) {
      if(wcounter == -1){
        LockResult woundresult = wound_writelock(tuple);

        if(woundresult == LockResult::ABORTED) { tuple->lock_.latch_unlock(wcounter); return Status::ERROR_LOCK_FAILED; }
        else if(woundresult == LockResult::NOT_FOUND) { tuple->lock_.latch_unlock(0); return Status::WARN_NOT_FOUND; }
        else if(woundresult == LockResult::SUCCESS) wcounter = 0;

      }else if(wcounter >= 1){
        wcounter = wound_readlock(tuple, wcounter);
        if (this->status_ == TransactionStatus::aborted) { tuple->lock_.latch_unlock(wcounter); return Status::ERROR_LOCK_FAILED; }
      }
    }

    if (wcounter == 0) {
      tuple->add_owner(thid_);
      wcounter = -1;
      acquired = true;
      if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
    }else{
      if (tuple->waiters_head == nullptr) increment_waitcount(tuple);
      this->wait_entry.insertInto(tuple, local_timestamp);
    }

  }else this->wait_entry.insertInto(tuple, local_timestamp);

  tuple->lock_.latch_unlock(wcounter);

  if (!acquired) {
    LockResult result = wait_writeop(tuple);
    if (result == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
    if (result == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;
  }

  this->write_set_.emplace_back(s, key, tuple, OpType::DELETE);

FINISH_DELETE:
#if ADD_ANALYSIS
  result_->local_write_latency_ += rdtscp() - start;
#endif
  return Status::OK;
}

Status TxExecutor::read_lock(Storage s, std::string_view key) {
  Tuple* tuple;
  tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
  ++result_->local_tree_traversal_;
#endif
  if (reconnoitering_) return Status::OK;

  if (tuple == nullptr) { return Status::WARN_NOT_FOUND; }

  for (auto& r_lock : r_lock_list_) {
    if (r_lock == &tuple->lock_) { return Status::OK; }
  }

  for (auto& w_lock : w_lock_list_) {
    if (w_lock == &tuple->lock_) { return Status::OK; }
  }

#ifdef DLR0
  tuple->lock_.r_lock();
#elif defined(DLR1)
  if (!tuple->lock_.r_trylock()) {
    this->status_ = TransactionStatus::aborted;
    return Status::ERROR_LOCK_FAILED;
  }
#endif
  r_lock_list_.emplace_back(&tuple->lock_);

  return Status::OK;
}

Status TxExecutor::write_lock(Storage s, std::string_view key) {
  Tuple* tuple;
  tuple = Masstrees[get_storage(s)].get_value(key);
#if ADD_ANALYSIS
  ++result_->local_tree_traversal_;
#endif
  if (tuple == nullptr) return Status::WARN_NOT_FOUND;

  for (auto& w_lock : w_lock_list_) {
    if (w_lock == &tuple->lock_) { return Status::OK; }
  }

#if DLR0
  tuple->lock_.w_lock();
#elif defined(DLR1)
  if (!tuple->lock_.w_trylock()) {
    this->status_ = TransactionStatus::aborted;
    return Status::ERROR_LOCK_FAILED;
  }
#endif
  w_lock_list_.emplace_back(&tuple->lock_);

  return Status::OK;
}

void TxExecutor::reconnoiter_begin() { reconnoitering_ = true; }

void TxExecutor::reconnoiter_end() {
  // unlockList(); この関数自体TPC-CとYCSBでは使われない.
  read_set_.clear();
  reconnoitering_ = false;
  begin();
}

bool TxExecutor::isLeader() { return this->thid_ == 0; }

void TxExecutor::leaderWork() {
#if BACK_OFF
  leaderBackoffWork(backoff_, CCBenchResults);
#endif
}

LockResult TxExecutor::wait_readop(Tuple* tuple) {
	while(true){

    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if (!this->wait_entry.is_head.load(memory_order_acquire)) { _mm_pause(); continue;}

    // headの操作
    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    // counterが期待する値になっているのかを調べる.
    int expected = tuple->lock_.counter.load(memory_order_acquire);

    bool try_wound = false;
    if (expected < 0) {
      // head(自分)より小さいtimestampを持つTXのみがロックをとっているという保証がない and 誰かが自分を待っておりサイクルの最小値になりうる → woundを試みる
      bool waited = this->waiter_count_.load() > 0 || this->wait_entry.next != nullptr;
      if (waited) {
        if (has_younger_owner(tuple, thid_, local_timestamp)) try_wound = true;
      }
      if (!try_wound) { _mm_pause(); continue; }
    }

    int result = tuple->lock_.latch_lock();

    if(tuple->delete_flag == true){
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(result);
      return LockResult::NOT_FOUND;
    }

    //latch内でまだheadなのかどうかを調べる.
    if (tuple->waiters_head != &this->wait_entry) {
      tuple->lock_.latch_unlock(result);
      continue;
    }

    //WriteLockがかかっているのでそれをwoundする
    if(try_wound && result == -1){
      LockResult woundresult = wound_writelock(tuple);

      if (woundresult == LockResult::ABORTED || woundresult == LockResult::NOT_FOUND) {
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
        tuple->lock_.latch_unlock(result);
        return woundresult;
      }
      else if (woundresult == LockResult::SUCCESS) result = 0;
    }

    if(result >= 0){
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
      else decrement_waitcount(tuple);
      tuple->add_owner(thid_);
      result++;
      tuple->lock_.latch_unlock(result);
      return LockResult::SUCCESS;
    }

    tuple->lock_.latch_unlock(result);
  }
}

LockResult TxExecutor::wait_writeop(Tuple* tuple) {
	while(true){
    
    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if (!this->wait_entry.is_head.load(memory_order_acquire)) { _mm_pause(); continue; }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    int expected = tuple->lock_.counter.load(memory_order_acquire);

    bool try_wound = false;
    if (expected != 0) {
      //headよりも小さいTSを持つTXのみLockを所持しているという保証がない and サイクルの最小値になりうる
      bool waited = this->waiter_count_.load() > 0 || this->wait_entry.next != nullptr;
      if (waited) {
        if (has_younger_owner(tuple, thid_, local_timestamp)) try_wound = true;
      }
      if (!try_wound) { _mm_pause(); continue; }
    }

    int result = tuple->lock_.latch_lock();

    if(tuple->delete_flag == true){
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(result);
      return LockResult::NOT_FOUND;
    }

    if(tuple->waiters_head != &this->wait_entry) {
      tuple->lock_.latch_unlock(result);
      continue;
    }

    if (try_wound) {
      if(result == -1){
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED || woundresult == LockResult::NOT_FOUND) {
          this->wait_entry.removeFrom(tuple);
          if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
          tuple->lock_.latch_unlock(result);
          return woundresult;
        }
        else if (woundresult == LockResult::SUCCESS) result = 0;

      }else if(result >= 1){
        result = wound_readlock(tuple, result);
        if(this->status_.load(std::memory_order_acquire) == TransactionStatus::aborted){
          this->wait_entry.removeFrom(tuple);
          if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
          tuple->lock_.latch_unlock(result);
          return LockResult::ABORTED;
        }
      }
    }

    if(result == 0){
      result = -1;
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
      else decrement_waitcount(tuple);
      tuple->add_owner(thid_);
      tuple->lock_.latch_unlock(result);
      return LockResult::SUCCESS;
    }

    tuple->lock_.latch_unlock(result);
  }
}

LockResult TxExecutor::wait_upgradeop(Tuple* tuple) {
	while(true){

    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if (!this->wait_entry.is_head.load(memory_order_acquire)) { _mm_pause(); continue; }

    // headの操作
    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    int expected = tuple->lock_.counter.load(memory_order_acquire);

    bool try_wound = false;
    if (expected != 1) {
      //upgrade時はheadに並ぶ際に自分自身も現在ownerとしてwaiter_count_を+1されているため,自分自身の分の+1を差し引いて判定する(> 1).
      bool waited = this->waiter_count_.load() > 1 || this->wait_entry.next != nullptr;
      if (waited) {
        if (has_younger_owner(tuple, thid_, local_timestamp)) try_wound = true;
      }
      if (!try_wound) { _mm_pause(); continue; }
    }

    int result = tuple->lock_.latch_lock();

    //statusを確認することによって,自分のReadLockが解放されていないことを保証する.
    if(this->status_ == TransactionStatus::aborted){
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(result);
      return LockResult::ABORTED;
    }

    if(tuple->waiters_head != &this->wait_entry){
      tuple->lock_.latch_unlock(result);
      continue;
    }

    if(try_wound && result > 1){
      result = wound_readlock(tuple, result);
      if(this->status_.load(std::memory_order_acquire) == TransactionStatus::aborted){
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::ABORTED;
      }
    }

    if(result == 1){
      result = -1;
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) decrement_waitcount(tuple);
      tuple->lock_.latch_unlock(result);
      return LockResult::SUCCESS;
    }
    tuple->lock_.latch_unlock(result);
  }
}

LockResult TxExecutor::wound_writelock(Tuple *tuple) {
  // counterとowners/bitapはlatchよってatomicに処理される.そのため,それらの値が整合していることは保証される.
  if (tuple->owners_bitmap != 0) {
    const int i = std::countr_zero(tuple->owners_bitmap);

    if(i != thid_){
      if(AllExecutors[i]->local_timestamp > local_timestamp){
        TransactionStatus expected = TransactionStatus::inflight;
        if(!AllExecutors[i]->status_.compare_exchange_strong(expected, TransactionStatus::aborted,memory_order_acq_rel, memory_order_acquire)){
          if(expected != TransactionStatus::aborted){
            this->status_.store(TransactionStatus::aborted, memory_order_release);
            return LockResult::ABORTED;
          }
        }

        tuple->del_owner(i); 
        // Woundに成功したならWoundしたTXがwoundされたTXが所持していたLockを外す.しかし,あるTXがinsertをしていた場合そのTXがwoundされるとそのrecordを読んではいけない.
        // それを実現するために,commitされていないrecordにはcommitted_recordがfalseになっている.WriteLockが取られていたので,woundしたがcommitted_record=falseならばNot_Foundを返す.
        if(!tuple->committed_record){
          tuple->delete_flag = true;
          return LockResult::NOT_FOUND;
        }
        return LockResult::SUCCESS;

      }else if(AllExecutors[i]->local_timestamp < local_timestamp){
        return LockResult::FAILED;
      }
    }
  }

  assert(false && "wound_writelock: counter==-1なのにownersに該当者がいない");
  return LockResult::FAILED;
}

int TxExecutor::wound_readlock(Tuple *tuple, int counter) {
  for (uint64_t bits = tuple->owners_bitmap; bits != 0; bits &= bits - 1) {
    const int i = std::countr_zero(bits);
	  if(i == thid_){ //自分をwoundしないため
      continue;
    }else if(AllExecutors[i]->local_timestamp > local_timestamp){
      TransactionStatus expected = TransactionStatus::inflight;
      if (!AllExecutors[i]->status_.compare_exchange_strong(expected, TransactionStatus::aborted,memory_order_acq_rel, memory_order_acquire)){
        if (expected != TransactionStatus::aborted) {
          this->status_.store(TransactionStatus::aborted, memory_order_release);
          return counter;
        }
      }

      tuple->del_owner(i);
      counter --;
    }
  }
  return counter;
}