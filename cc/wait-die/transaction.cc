
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

  /* Release locks
   * wait-dieでは他のTXが自分のLockを解放することはないので, 取得したLockとowners_bitmapは
   * 全てこのTXが解放する. */

	for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
		Tuple* tuple = (*itr).rcdptr_;
	  int prev = tuple->lock_.latch_lock();

    /* counterをチェックして counter=-1 ならskipする. Upgradeが発生していることを意味し,WriteSetのループで解放する. */
		if(prev != -1){
			tuple->del_owner(thid_);
			prev -= 1;
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

		  tuple->del_owner(thid_);
		  prev = 0;
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

	status_ = TransactionStatus::committed;

  /* 取得した Lockとowners_bitmap は全てこのTXが解放する */

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
  if (!is_retry) this->local_timestamp = TimestampCounter.fetch_add(1, std::memory_order_relaxed);
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

LockResult TxExecutor::read_internal(Storage s, std::string_view key, Tuple* tuple, int rcounter) {
  TupleBody body;

  if (reconnoitering_) goto FINISH_READ_LOCK;

  int owner_id;
  LockResult result;

  /* 待ち行列が空. 互換性とdie判定だけで決まる */
  if(tuple->waiters_head == nullptr){

    /* ownerはreaderのみ(または空). ReadLock同士は互換なので直接取得する */
    if(rcounter >= 0){
      tuple->add_owner(thid_);
      rcounter++;
      tuple->lock_.latch_unlock(rcounter);
      goto FINISH_READ_LOCK;
    }

    /* rcounter == -1: 他のTXがWriteLockを保持している. ownerはちょうど1人.
     * そのownerが自分より古い(tsが小さい)なら自分がdieする. 若いなら待てる. */
    owner_id = std::countr_zero(tuple->owners_bitmap);
    if(AllExecutors[owner_id]->local_timestamp < this->local_timestamp){
      this->status_ = TransactionStatus::aborted;
      tuple->lock_.latch_unlock(rcounter);
      return LockResult::ABORTED;
    }

  /* 待ち行列が非空. headを奪う位置に入るならdieする(Read要求の追い越し禁止).
   * my_ts < head->ts なら不変条件より全ownerより古いことが保証されるのでdie判定は不要. */
  }else{
    if(this->local_timestamp > tuple->waiters_head->ts){
      this->status_ = TransactionStatus::aborted;
      tuple->lock_.latch_unlock(rcounter);
      return LockResult::ABORTED;
    }
  }

  this->wait_entry.insertInto(tuple, this->local_timestamp);
  tuple->lock_.latch_unlock(rcounter);

  result = wait_readop(tuple);
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

    //既にTXが読んだことがある
    SetElement<Tuple>* e = searchReadSet(s, itr->body_.get_key());
    if (e) {
      result.emplace_back(&(e->body_));
      continue;
    }

    //既にTXが書き込んだことがあるか
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
    if(readresult == LockResult::ABORTED) return Status::ERROR_LOCK_FAILED;
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

      /* 自分だけがReadLockを持っている状態. die判定の相手となる他のownerがいないので
       * そのままWriteLockに昇格する. waiterがいれば追い越すことになるが, 不変条件より
       * waiterは全員自分より古いので, 昇格後は「古いTXが若い自分を待つ」形になり壊れない. */
      if (upcounter == 1){
        upcounter = -1;
        (*rItr).rcdptr_->add_owner(thid_);
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        write_set_.emplace_back(s, key, (*rItr).rcdptr_, std::move(body),OpType::UPDATE);
        goto FINISH_WRITE;

      //他のTXもReadLockをとっていた
      }else if(upcounter >= 1){
        /* 自分より古いownerが一人でもいれば自分がdieする. */
        for(uint64_t bits = (*rItr).rcdptr_->owners_bitmap; bits != 0; bits &= bits - 1){
          int i = std::countr_zero(bits);
          if(AllExecutors[i]->local_timestamp < this->local_timestamp){
            this->status_ = TransactionStatus::aborted;
            (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
            return Status::ERROR_LOCK_FAILED;
          }
        }

        this->wait_entry.insertInto((*rItr).rcdptr_, local_timestamp);
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);

        LockResult result = wait_upgradeop((*rItr).rcdptr_);
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

  /* ロックが空いていて, かつ待っているTXが全員自分より若いなら直接取得する.
   * 自分より古いwaiterがいるのに取得すると, そのwaiterが自分を待つことになり
   * 「若いTXが古いTXを待つ」形になって不変条件が壊れる. */
  if(wcounter == 0 && (tuple->waiters_head == nullptr || tuple->waiters_head->ts < this->local_timestamp)){
    tuple->add_owner(thid_);
    wcounter = -1;
    acquired = true;

  /* WriteLockは必ず競合するのでdie判定を受ける. 自分より古いownerが一人でもいれば自分がdieする.
   * wcounter == 0 の場合はownerがいないのでループは空回りし, そのまま待ち行列に入る. */
  }else{
    for(uint64_t bits = tuple->owners_bitmap; bits != 0; bits &= bits - 1){
      int i = std::countr_zero(bits);
      if(AllExecutors[i]->local_timestamp < this->local_timestamp){
        this->status_ = TransactionStatus::aborted;
        tuple->lock_.latch_unlock(wcounter);
        return Status::ERROR_LOCK_FAILED;
      }
    }

    this->wait_entry.insertInto(tuple, local_timestamp);
  }
  

  tuple->lock_.latch_unlock(wcounter);

  if (!acquired) {
    LockResult result = wait_writeop(tuple);
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
  // delete_flagはデフォルトのfalseのまま. このTXがabortした場合, abort()がMasstreeから外して
  // delete_flag=trueにするので, 待っていたreaderはwait_readopでNOT_FOUNDを受け取る.

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

      /* 自分だけがReadLockを持っている状態. die判定の相手となる他のownerがいないので
       * そのままWriteLockに昇格する. waiterがいれば追い越すことになるが, 不変条件より
       * waiterは全員自分より古いので, 昇格後は「古いTXが若い自分を待つ」形になり壊れない. */
      if (upcounter == 1){
        upcounter = -1;
        (*rItr).rcdptr_->add_owner(thid_);
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        write_set_.emplace_back(s, key, (*rItr).rcdptr_, OpType::DELETE);
        goto FINISH_DELETE;

      }else if(upcounter >= 1){

        /* 自分より古いownerが一人でもいれば自分がdieする. */
        for(uint64_t bits = (*rItr).rcdptr_->owners_bitmap; bits != 0; bits &= bits - 1){
          int i = std::countr_zero(bits);
          if(AllExecutors[i]->local_timestamp < this->local_timestamp){
            this->status_ = TransactionStatus::aborted;
            (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
            return Status::ERROR_LOCK_FAILED;
          }
        }

        this->wait_entry.insertInto((*rItr).rcdptr_, local_timestamp);
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);

        LockResult result = wait_upgradeop((*rItr).rcdptr_);
        if(result == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;

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

  /* ロックが空いていて, かつ待っているTXが全員自分より若いなら直接取得する.
   * 自分より古いwaiterがいるのに取得すると, そのwaiterが自分を待つことになり
   * 「若いTXが古いTXを待つ」形になって不変条件が壊れる. */
  if(wcounter == 0 && (tuple->waiters_head == nullptr || tuple->waiters_head->ts < this->local_timestamp)){
    tuple->add_owner(thid_);
    wcounter = -1;
    acquired = true;

  /* WriteLockは必ず競合するのでdie判定を受ける. 自分より古いownerが一人でもいれば自分がdieする.
   * wcounter == 0 の場合はownerがいないのでループは空回りし, そのまま待ち行列に入る. */
  }else{
    for(uint64_t bits = tuple->owners_bitmap; bits != 0; bits &= bits - 1){
      int i = std::countr_zero(bits);
      if(AllExecutors[i]->local_timestamp < this->local_timestamp){
        this->status_ = TransactionStatus::aborted;
        tuple->lock_.latch_unlock(wcounter);
        return Status::ERROR_LOCK_FAILED;
      }
    }

    this->wait_entry.insertInto(tuple, local_timestamp);
  }

  tuple->lock_.latch_unlock(wcounter);

  if (!acquired) {
    LockResult result = wait_writeop(tuple);
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

    if (!this->wait_entry.is_head.load(memory_order_acquire)) {
      _mm_pause();
      continue;
    }

    // これ以降はheadの操作
    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

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
        tuple->add_owner(thid_);
		    result ++;
        this->wait_entry.removeFrom(tuple);
		    tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else  tuple->lock_.latch_unlock(result);
    
    }else _mm_pause();
  }
}

LockResult TxExecutor::wait_writeop(Tuple* tuple) {
	while(true){

    if (!this->wait_entry.is_head.load(memory_order_acquire)) {
      _mm_pause();
      continue;
    }

    // headの操作
    if(tuple->delete_flag == true){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }
    
    int expected = tuple->lock_.counter.load(memory_order_acquire);
    if (expected == 0){
      int result = tuple->lock_.latch_lock();
      if(tuple->delete_flag == true){
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::NOT_FOUND;
      }

      if(tuple->waiters_head != &this->wait_entry) {
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result == 0){
        tuple->add_owner(thid_);
        result = -1;
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else  tuple->lock_.latch_unlock(result);
      
    }else _mm_pause();
  }
}

LockResult TxExecutor::wait_upgradeop(Tuple* tuple) {
	while(true){

    if (!this->wait_entry.is_head.load(memory_order_acquire)) {
      _mm_pause();
      continue;
    }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    int expected = tuple->lock_.counter.load(memory_order_acquire);
    if (expected == 1) {
      int result = tuple->lock_.latch_lock();

      if (tuple->waiters_head != &this->wait_entry) {
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result == 1){
        result = -1;
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;

      }else tuple->lock_.latch_unlock(result);
      
    } else _mm_pause();
  }
}
