
#include <stdio.h>
#include <string.h>
#include <cstdarg>

#include <atomic>

#include "../../include/backoff.hh"
#include "../../include/debug.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "include/common.hh"
#include "include/transaction.hh"
#include "include/timestamp.hh"

using namespace std;

extern void display_procedure_vector(std::vector<Procedure>& pro);

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

  /* Release locks */
	for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
		  Tuple* tuple = (*itr).rcdptr_;
		  int prev = tuple->lock_.latch_lock();
		  if(tuple->owners[thid_]!= -1){ //順番を気をつける必要がある.なぜなら,自分がownersにいないとその-1は自分かどうか判定できない.
				if(prev != -1){               //writelockがかかっていない. つまり,upgradeされていないためreadlockがかかっている.
          tuple->owners[thid_] = -1;
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

		  if(tuple->owners[thid_]!= -1){
				tuple->owners[thid_] = -1;
				prev = 0;
			}
		  tuple->lock_.latch_unlock(prev);
	}

  /* Clean-up local read/write set.*/
  read_set_.clear();
  write_set_.clear();

  ++result_->local_abort_counts_;

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
	// memory fence
	if (!success) return false; //tpcc.hh内でcommit=falseの戻り値なら自動的にabort()を実行する

	for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
		  Tuple* tuple = (*itr).rcdptr_;

		  int prev = tuple->lock_.latch_lock();

		  if(prev != -1){
			  	tuple->owners[thid_] = -1;
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

      /* latch・lock解放　*/
      tuple->owners[thid_] = -1;
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
  this->status_ = TransactionStatus::inflight;
  this->local_timestamp = TimestampCounter.fetch_add(1, std::memory_order_relaxed);
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
  //if(readresult == LockResult::SUCCESS) {};

  *body = &(read_set_.back().body_);

FINISH_READ:
#if ADD_ANALYSIS
  result_->local_read_latency_ += rdtscp() - start;
#endif
  return Status::OK;
}

LockResult TxExecutor::read_internal(Storage s, std::string_view key, Tuple* tuple, int rcounter) {
  TupleBody body;

  if (reconnoitering_) goto FINISH_READ_LOCK;

  if(tuple->waiters_head == nullptr || tuple->waiters_head->ts > this->local_timestamp){
    if(rcounter >= 0){

      tuple->owners[thid_] = local_timestamp;
      rcounter++;
      tuple->lock_.latch_unlock(rcounter);
      goto FINISH_READ_LOCK;

    }else if(rcounter == -1){ //writelockが取られていた
      LockResult woundresult;
      woundresult = wound_writelock(tuple);
      if(woundresult == LockResult::ABORTED){
        tuple->lock_.latch_unlock(rcounter);
        return LockResult::ABORTED;
      }else if(woundresult == LockResult::SUCCESS){
        rcounter = 0;
      }else if(woundresult == LockResult::FAILED){}
    }
  }
  this->wait_entry.insertInto(tuple, this->local_timestamp);
  tuple->lock_.latch_unlock(rcounter);

  /*
  * headよりも大きいtimestampならばwound操作をせず,そのままwaiter_listにinsertする挙動になっている
  * 理由 :
  *   (read・write・deleteに関わらず) wait_listのheadに挿入する時には必ずwoundを行う.
  *   そして,waiter-listはtimestmap順でsortされておりその順番でロックを取得する.
  *   そのため,常にheadにいるTXのtimestampより大きいTXがLockをしていないことを保証できる.
  *   よって,それより大きいtimestampを持つTXがwoundしても意味がない.
  */

  LockResult result;
  result = wait_readop(tuple);
  if (result == LockResult::SUCCESS) goto FINISH_READ_LOCK;
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
  auto rset_init_size = read_set_.size();

  std::vector<Tuple*> scan_res;
  Masstrees[get_storage(s)].scan(
      left_key.empty() ? nullptr : left_key.data(), left_key.size(),
      l_exclusive, right_key.empty() ? nullptr : right_key.data(),
      right_key.size(), r_exclusive, &scan_res, limit);

  for (auto&& itr : scan_res) {
    SetElement<Tuple>* e = searchReadSet(s, itr->body_.get_key());//既にこのトランザクションが読んだことがある
    if (e) {
      result.emplace_back(&(e->body_));
      continue;
    }

    e = searchWriteSet(s, itr->body_.get_key());//既にこのトランザクションが書き込んだことがあるか
    if (e) {
      result.emplace_back(&(e->body_));
      continue;
    }

    int rcounter = itr->lock_.latch_lock();
    if(itr->delete_flag == true){
      itr->lock_.latch_unlock(rcounter);
      return Status::WARN_NOT_FOUND;
    }
    LockResult readresult = read_internal(s, itr->body_.get_key(), itr, rcounter);
    if(readresult == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;
    if (readresult == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
  }

  if (rset_init_size != read_set_.size()) {
    for (auto itr = read_set_.begin() + rset_init_size; itr != read_set_.end();++itr) {
      result.emplace_back(&((*itr).body_));
    }
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

      //上でstatus=abortedを保証しないと,自分がreadlockをとっている. つまり,upcounterの値が1以上であることを保証できない.
      TransactionStatus ts = status_.load(std::memory_order_acquire);
      if(ts == TransactionStatus::aborted){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::ERROR_LOCK_FAILED;
      }

      // if ((*rItr).rcdptr_->waiters_head == nullptr || (*rItr).rcdptr_->waiters_head->ts > this->local_timestamp)
      // headよりも大きいTSを持つTXはロックを取れないことが保証されている. つまり, 自分がreadlockを持っているならheadは自分よりTSが大きい.
      // よって,read-setにhitしていながらheadよりもTSが大きくなることなはない.

      if (upcounter == 1){

        upcounter = -1;
        (*rItr).rcdptr_->owners[thid_] = local_timestamp;
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        write_set_.emplace_back(s, key, (*rItr).rcdptr_, std::move(body),OpType::UPDATE);
        goto FINISH_WRITE;

      }else if(upcounter >= 1){

        this->wait_entry.insertInto((*rItr).rcdptr_, local_timestamp);

        upcounter = wound_readlock((*rItr).rcdptr_,upcounter);
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        if(this->status_ == TransactionStatus::aborted){
          int r = (*rItr).rcdptr_->lock_.latch_lock();
          this->wait_entry.removeFrom((*rItr).rcdptr_);
          (*rItr).rcdptr_->lock_.latch_unlock(r);
          return Status::ERROR_LOCK_FAILED;
        }

        LockResult result = wait_upgradeop((*rItr).rcdptr_);
        if (this->status_ == TransactionStatus::aborted || result == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
        if(result == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;

        write_set_.emplace_back(s, key, (*rItr).rcdptr_, std::move(body),OpType::UPDATE);

        goto FINISH_WRITE;
      }
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
  if (tuple->waiters_head == nullptr || tuple->waiters_head->ts > this->local_timestamp) {

    if (wcounter == 0) {
      tuple->owners[thid_] = local_timestamp;
      wcounter = -1;
      acquired = true;

    }else{

      if (wcounter == -1){

        LockResult woundresult;
        woundresult = wound_writelock(tuple);

        if(woundresult == LockResult::ABORTED){
          tuple->lock_.latch_unlock(wcounter);
          return Status::ERROR_LOCK_FAILED;
        }else if(woundresult == LockResult::SUCCESS){
          wcounter = 0;
        }else if(woundresult == LockResult::FAILED){}

      }else if (wcounter >= 1){
        wcounter = wound_readlock(tuple, wcounter);
      }

      this->wait_entry.insertInto(tuple, local_timestamp);
    }

  }else{
    this->wait_entry.insertInto(tuple, local_timestamp);
  }

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
  tuple->owners[this->thid_] = local_timestamp;

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

      TransactionStatus ts = status_.load(std::memory_order_acquire);
      if(ts == TransactionStatus::aborted){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::ERROR_LOCK_FAILED;
      }

      // if ((*rItr).rcdptr_->waiters_head == nullptr || (*rItr).rcdptr_->waiters_head->ts > this->local_timestamp){
        if (upcounter == 1){

          upcounter = -1;
          (*rItr).rcdptr_->owners[thid_] = local_timestamp;
          (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, (*rItr).rcdptr_, OpType::DELETE);
          goto FINISH_DELETE;

        }else if(upcounter >= 1){

          this->wait_entry.insertInto((*rItr).rcdptr_, local_timestamp);
          upcounter = wound_readlock((*rItr).rcdptr_,upcounter);
          (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
          LockResult result = wait_upgradeop((*rItr).rcdptr_);
          if(result == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
          if(result == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;  //しかし,これはすでに自分がabortされているということ.

          write_set_.emplace_back(s, key, (*rItr).rcdptr_, OpType::DELETE);
          goto FINISH_DELETE;

        }

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

  if(tuple->delete_flag == true) {
    tuple->lock_.latch_unlock(wcounter);
    return Status::WARN_NOT_FOUND;
  }

  bool acquired;
  acquired = false;
  if (tuple->waiters_head == nullptr || tuple->waiters_head->ts > this->local_timestamp) {

    if (wcounter == 0) {

      tuple->owners[thid_] = local_timestamp;
      wcounter = -1;
      acquired = true;

    } else {

      if (wcounter == -1){

        LockResult woundresult;
        woundresult = wound_writelock(tuple);
        if(woundresult == LockResult::ABORTED){
          tuple->lock_.latch_unlock(wcounter);
          return Status::ERROR_LOCK_FAILED;
        }else if(woundresult == LockResult::SUCCESS){
          wcounter = 0;
        }else if(woundresult == LockResult::FAILED){}

      }
      else if (wcounter >= 1){
        wcounter = wound_readlock(tuple, wcounter);
      }

      this->wait_entry.insertInto(tuple, local_timestamp);
    }

  }else{
    this->wait_entry.insertInto(tuple, local_timestamp);
  }

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

/**
 * @brief unlock and clean-up local lock set.
 * @return void
 */
void TxExecutor::unlockList() {
  for (auto itr = r_lock_list_.begin(); itr != r_lock_list_.end(); ++itr)
    (*itr)->r_unlock();

  for (auto itr = w_lock_list_.begin(); itr != w_lock_list_.end(); ++itr)
    (*itr)->w_unlock();

  /**
   * Clean-up local lock set.
   */
  r_lock_list_.clear();
  w_lock_list_.clear();
}

void TxExecutor::reconnoiter_begin() { reconnoitering_ = true; }

void TxExecutor::reconnoiter_end() {
  unlockList();
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

// wound-wait用に追加
LockResult TxExecutor::wait_readop(Tuple* tuple) {
	while(true){

    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    if (tuple->waiters_head != &this->wait_entry) {
      continue;
    }

    // 3. counterが使用可能か確認
    int expected = tuple->lock_.counter.load(memory_order_acquire);

    if (expected >= 0) {
      int result = tuple->lock_.latch_lock();

      if(tuple->delete_flag == true){

        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::NOT_FOUND;

      }

      if (tuple->waiters_head != &this->wait_entry) {
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result >= 0){

        tuple->owners[thid_] = local_timestamp;
		    result ++;
        this->wait_entry.removeFrom(tuple);
		    tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;

      }else{
        tuple->lock_.latch_unlock(result);
      }
    }
  }
}

LockResult TxExecutor::wait_writeop(Tuple* tuple) {//wait until an operation ends
	while(true){

    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    if (tuple->waiters_head != &this->wait_entry) {
      continue;
    }

    int expected = tuple->lock_.counter.load(memory_order_acquire);

    if (expected == 0) {

      int result = tuple->lock_.latch_lock();
      if(tuple->delete_flag == true){

        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::NOT_FOUND;

      }

      if (tuple->waiters_head != &this->wait_entry) {
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result == 0){

        tuple->owners[thid_] = local_timestamp;
        result = -1;
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else{
        tuple->lock_.latch_unlock(result);
      }
    }
  }
}

LockResult TxExecutor::wait_upgradeop(Tuple* tuple) {
	while(true){
    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if (tuple->waiters_head != &this->wait_entry) continue;

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    // 3. counterが使用可能か確認
    int expected = tuple->lock_.counter.load(memory_order_acquire);
    if (expected == 1) {

      int result = tuple->lock_.latch_lock();

      //ここでstatusを確認することによって, 自分のreadlockが取られていることを保証する.
      if(this->status_ == TransactionStatus::aborted){
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::ABORTED;
      }

      if (tuple->waiters_head != &this->wait_entry) {
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result == 1){
        result = -1;
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else{
        tuple->lock_.latch_unlock(result);
      }
    }
  }
}

LockResult TxExecutor::wound_writelock(Tuple *tuple) {
	for (uint32_t i = 0; i < TotalThreadNum; i++) {
    if(tuple->owners[i] == -1){
      continue;

    /* counterとownersはlatchよってatomicに処理される. そのため,それらが整合していることは保証される.
     * counter = -1 ならownersにtimestampを登録しているのは一つのTXしかない. よって,それに当たると即座に終了して良い.*/
    }else if (tuple->owners[i] > local_timestamp) {
      TransactionStatus expected = TransactionStatus::inflight;
      if (!AllExecutors[i]->status_.compare_exchange_strong(expected, TransactionStatus::aborted,memory_order_acq_rel, memory_order_acquire)) {
        if(expected == TransactionStatus::aborted){
          tuple->owners[i] = -1;
          return LockResult::SUCCESS;
        }
        this->status_.store(TransactionStatus::aborted, memory_order_release);
        return LockResult::ABORTED;
      }else{
        tuple->owners[i] = -1;
        return LockResult::SUCCESS;
      }
    }else if(tuple->owners[i] < local_timestamp){
      return LockResult::FAILED;
    }
  }
  assert(false && "wound_writelock: counter==-1なのにownersに該当者がいない");
  return LockResult::FAILED;
}

int TxExecutor::wound_readlock(Tuple *tuple, int counter) {
	for (uint32_t i = 0; i < TotalThreadNum; i++) {
	  if(tuple->owners[i] == -1 || tuple->owners[i] == local_timestamp){ //自分をwoundしないため
      continue;
    }else if (tuple->owners[i] > local_timestamp) {
      TransactionStatus expected = TransactionStatus::inflight;
      if (!AllExecutors[i]->status_.compare_exchange_strong(expected, TransactionStatus::aborted,memory_order_acq_rel, memory_order_acquire)){
        if(expected == TransactionStatus::aborted){
          tuple->owners[i] = -1;
          counter --;
          continue;
        }
        this->status_.store(TransactionStatus::aborted, memory_order_release);
        return counter;
      }else{ //CAS成功
        tuple->owners[i] = -1;
        counter --;
      }
    }
  }
  return counter;
}
