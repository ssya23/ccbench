
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

  /* Release locks 
   * abortされたTXは他のthreadに Lock&owners を解放されている可能性がある. よって,latch取得後以下の二つの挙動のどちらかを取る
   * 1.Lock&ownersがまだ残されている場合, 解放する.
   * 2.Lock&ownersがすでに他のthreadによって解放されているのでskip */

	for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr) {
		Tuple* tuple = (*itr).rcdptr_;
	  int prev = tuple->lock_.latch_lock();

    /* ReadSetのループでは,ownersを確認して自分のTSがowners[thid_]に-1が入っていたらownersとLockが解放されている. 
     * owners[thid_]に自分のTSが入っていれば自分のLockがまだ解放されていない.counterをチェックしてcounter=-1ならskipする.
     * それは, upgradeが発生していることになりWriteSetのループで解放する. 
     * またこの時先にowenrsを確認している必要がある.なぜなら,自分がownersにいないとcounterの-1は自分が取得したWriteLockかどうか判定できない */
      
		if(tuple->owners[thid_]!= -1){ 
			if(prev != -1){               
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
	if (!success) return false; //tpcc.hh or ycsb.hh内でcommit()の戻り値がfalseならabort()が実行される. 

  /* 取得したLock&ownersは全てこのthreadが解放する */

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
          tuple->delete_flag = false;
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
  bool is_retry = (this->status_ == TransactionStatus::aborted);
  this->status_ = TransactionStatus::inflight;
  fprintf(stderr, "[BEGIN] thid=%d is_retry=%d old_ts=%d old_wc=%d\n", thid_, is_retry, local_timestamp, this->waiter_count_.load());
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

LockResult TxExecutor::read_internal(Storage s, std::string_view key, Tuple* tuple, int rcounter) {
  TupleBody body;

  if (reconnoitering_) goto FINISH_READ_LOCK;

  if(tuple->waiters_head == nullptr){
    // WaitListに誰もいない
    if (rcounter >= 0) {
      tuple->owners[thid_] = local_timestamp;
      rcounter++;
      tuple->owner_older = true;
      tuple->lock_.latch_unlock(rcounter);
      goto FINISH_READ_LOCK;
    }else if (rcounter == -1){
      if (this->waiter_count_.load() > 0) { // 自分が既に誰かに待たれている → wound実行
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED) {
          tuple->lock_.latch_unlock(rcounter);
          return LockResult::ABORTED;
        }else if(woundresult == LockResult::SUCCESS){
          rcounter = 1;
          tuple->owners[thid_] = local_timestamp;
          tuple->owner_older = true;
          tuple->lock_.latch_unlock(rcounter);
          goto FINISH_READ_LOCK;
        }else if(woundresult == LockResult::FAILED) tuple->owner_older = true;
      
      }else tuple->owner_older = false; // WoundしないままWaitする  
    }

  }else if(tuple->waiters_head->ts > this->local_timestamp){
    if(rcounter >= 0){
      tuple->owners[thid_] = local_timestamp;
      rcounter++;
      this->waiter_count_.fetch_add(1, memory_order_acq_rel); // 同じWaitListで自分を待っているTXが存在している.
      tuple->lock_.latch_unlock(rcounter);
      goto FINISH_READ_LOCK;

    // WaitListのheadとしてInsert. 同じWaitListで他のTXが自分を待っている.そのためwoundする.
    }else if(rcounter == -1){
      LockResult woundresult = wound_writelock(tuple);

      if(woundresult == LockResult::ABORTED){
        tuple->lock_.latch_unlock(rcounter);
        return LockResult::ABORTED;
      }else if (woundresult == LockResult::SUCCESS){
        rcounter = 1;
        tuple->owners[thid_] = local_timestamp;
        this->waiter_count_.fetch_add(1, memory_order_acq_rel);
        tuple->owner_older = true;
        tuple->lock_.latch_unlock(rcounter);
        goto FINISH_READ_LOCK;
      }else if(woundresult == LockResult::FAILED) tuple->owner_older = true;
    }
  }
  // else(head_TS < 自分のTS)なら特に何もしない

  if(tuple->waiters_head == nullptr){
    // 自分が最初のwaiterになる → 現在の所有者(複数のことがある)のwaiter_count_を上げる
    for (uint32_t i = 0; i < TotalThreadNum; i++) {
      if (static_cast<int>(i) == thid_) continue;
      if (tuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
    }
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
      return Status::WARN_NOT_FOUND;
    }

    LockResult readresult = read_internal(s, itr->body_.get_key(), itr, rcounter);
    if(readresult == LockResult::NOT_FOUND) return Status::WARN_NOT_FOUND;
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

      TransactionStatus ts = status_.load(std::memory_order_acquire);
      if(ts == TransactionStatus::aborted){
        (*rItr).rcdptr_->lock_.latch_unlock(upcounter);
        return Status::ERROR_LOCK_FAILED;
        /* status!=abortedを保証しないと,counterの値が1でもそれは自分が取得しているreadlockであるという保証ができない. 
       　* もしも,statusをcheckした後abortされても問題はない.自分のTXはこのrecordに対してlatchを取っているので他のTXは勝手にこのrecordに対するLockとownersの値を解放できない.*/
      }

      Tuple* utuple = (*rItr).rcdptr_;

      if (utuple->waiters_head == nullptr){
        // WaitListに誰もいない
        if(upcounter == 1){
          upcounter = -1;
          utuple->owners[thid_] = local_timestamp;
          utuple->owner_older = true;
          utuple->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, utuple, std::move(body),OpType::UPDATE);
          goto FINISH_WRITE;

        }else if(this->waiter_count_.load() > 0){
          for (uint32_t i = 0; i < TotalThreadNum; i++) {
            if (static_cast<int>(i) == thid_) continue;
            if (utuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)utuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
          }
          this->wait_entry.insertInto(utuple, local_timestamp);
          upcounter = wound_readlock(utuple, upcounter);
          utuple->owner_older = true;
        }else{
          for (uint32_t i = 0; i < TotalThreadNum; i++) {
            if (static_cast<int>(i) == thid_) continue;
            if (utuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)utuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
          }
          this->wait_entry.insertInto(utuple, local_timestamp);
          utuple->owner_older = false; // WoundせずにWaitする
        }

      }else if(utuple->waiters_head->ts > this->local_timestamp){
        if(upcounter == 1){
          upcounter = -1;
          utuple->owners[thid_] = local_timestamp;
          this->waiter_count_.fetch_add(1, memory_order_acq_rel);
          utuple->owner_older = true;
          utuple->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, utuple, std::move(body),OpType::UPDATE);
          goto FINISH_WRITE;

        }else{
          this->wait_entry.insertInto(utuple, local_timestamp);
          upcounter = wound_readlock(utuple, upcounter);
          utuple->owner_older = true;
        }

      }else this->wait_entry.insertInto(utuple, local_timestamp);

      utuple->lock_.latch_unlock(upcounter);

      if(this->status_ == TransactionStatus::aborted){
        int r = utuple->lock_.latch_lock();
        this->wait_entry.removeFrom(utuple);
        utuple->lock_.latch_unlock(r);
        return Status::ERROR_LOCK_FAILED;
      }

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

  if (tuple->waiters_head == nullptr) {
    //WaitListに誰もいない.
    if (wcounter == 0) {
      tuple->owners[thid_] = local_timestamp;
      wcounter = -1;
      acquired = true;
    }else if(this->waiter_count_.load() > 0){
      if(wcounter == -1) {
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED) {
          tuple->lock_.latch_unlock(wcounter);
          return Status::ERROR_LOCK_FAILED;

        }else if(woundresult == LockResult::SUCCESS){
          tuple->owners[thid_] = local_timestamp;
          wcounter = -1;
          acquired = true;

        }else if(woundresult == LockResult::FAILED) tuple->owner_older = true;
      
      }else if(wcounter >= 1){ 
        wcounter = wound_readlock(tuple, wcounter);
        tuple->owner_older = true; 
      }
    
    }else tuple->owner_older = false; // woundせずにWait

    if(!acquired){
      // 自分が最初のwaiterになる → 現在の所有者(複数のことがある)のwaiter_count_を上げる
      for (uint32_t i = 0; i < TotalThreadNum; i++) {
        if (static_cast<int>(i) == thid_) continue;
        if (tuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
      }
      this->wait_entry.insertInto(tuple, local_timestamp);
    }

  }else if(tuple->waiters_head->ts > this->local_timestamp){
    if (wcounter == 0) {
      tuple->owners[thid_] = local_timestamp;
      wcounter = -1;
      acquired = true;
      this->waiter_count_.fetch_add(1, memory_order_acq_rel);
      tuple->owner_older = true;

    }else if(wcounter == -1){
      LockResult woundresult = wound_writelock(tuple);
      if (woundresult == LockResult::ABORTED) {
        tuple->lock_.latch_unlock(wcounter);
        return Status::ERROR_LOCK_FAILED;

      }else if(woundresult == LockResult::SUCCESS) {
        tuple->owners[thid_] = local_timestamp;
        wcounter = -1;
        acquired = true;
        this->waiter_count_.fetch_add(1, memory_order_acq_rel);

      }
      // SUCCESS/FAILEDいずれでも確定(既存headのため)
      tuple->owner_older = true; 

    }else{ // wcounter >= 1
      wcounter = wound_readlock(tuple, wcounter);
      tuple->owner_older = true;
    }
    
    if (!acquired) this->wait_entry.insertInto(tuple, local_timestamp);

  }else this->wait_entry.insertInto(tuple, local_timestamp);// owner_olderは既存headのための値のまま

  tuple->lock_.latch_unlock(wcounter);

  if (!acquired) {
    // wound_readlockは自分がabortされたことを戻り値では区別できないため,ここで確認する
    if (this->status_ == TransactionStatus::aborted) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return Status::ERROR_LOCK_FAILED;
    }

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
  tuple->delete_flag = true; //wound-waitでは他のthreadによってTXがabortされLockを外される可能性がある.その際にInsertされたrecordは存在してはいけないためdelete_flag=trueにしておく.commit時にdelete_flag=falseに戻す
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

      if (utuple->waiters_head == nullptr){
        // WaitListに誰もいない
        if(upcounter == 1){
          upcounter = -1;
          utuple->owners[thid_] = local_timestamp;
          utuple->owner_older = true;
          utuple->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, utuple, OpType::DELETE);
          goto FINISH_DELETE;

        }else if(this->waiter_count_.load() > 0){

          for(uint32_t i = 0; i < TotalThreadNum; i++){
            if (static_cast<int>(i) == thid_) continue;
            if (utuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)utuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
          }
          this->wait_entry.insertInto(utuple, local_timestamp);
          upcounter = wound_readlock(utuple, upcounter);
          utuple->owner_older = true;
        }else{
          for (uint32_t i = 0; i < TotalThreadNum; i++) {
            if (static_cast<int>(i) == thid_) continue;
            if (utuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)utuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
          }
          this->wait_entry.insertInto(utuple, local_timestamp);
          utuple->owner_older = false; // WoundせずにWaitする
        }

      }else if(utuple->waiters_head->ts > this->local_timestamp){
        if(upcounter == 1){
          upcounter = -1;
          utuple->owners[thid_] = local_timestamp;
          this->waiter_count_.fetch_add(1, memory_order_acq_rel);
          utuple->owner_older = true;
          utuple->lock_.latch_unlock(upcounter);
          write_set_.emplace_back(s, key, utuple, OpType::DELETE);
          goto FINISH_DELETE;

        }else{
          this->wait_entry.insertInto(utuple, local_timestamp);
          upcounter = wound_readlock(utuple, upcounter);
          utuple->owner_older = true;
        }

      }else this->wait_entry.insertInto(utuple, local_timestamp);

      utuple->lock_.latch_unlock(upcounter);

      if(this->status_ == TransactionStatus::aborted){
        int r = utuple->lock_.latch_lock();
        this->wait_entry.removeFrom(utuple);
        utuple->lock_.latch_unlock(r);
        return Status::ERROR_LOCK_FAILED;
      }

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

  if(tuple->delete_flag == true) {
    tuple->lock_.latch_unlock(wcounter);
    return Status::WARN_NOT_FOUND;
  }

  bool acquired;
  acquired = false;

  if (tuple->waiters_head == nullptr) {
    //WaitListに誰もいない.
    if (wcounter == 0) {
      tuple->owners[thid_] = local_timestamp;
      wcounter = -1;
      acquired = true;
    }else if(this->waiter_count_.load() > 0){
      if(wcounter == -1) {
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED) {
          tuple->lock_.latch_unlock(wcounter);
          return Status::ERROR_LOCK_FAILED;

        }else if(woundresult == LockResult::SUCCESS){
          tuple->owners[thid_] = local_timestamp;
          wcounter = -1;
          acquired = true;

        }else if(woundresult == LockResult::FAILED) tuple->owner_older = true;

      }else if(wcounter >= 1){
        wcounter = wound_readlock(tuple, wcounter);
        tuple->owner_older = true;
      }

    }else tuple->owner_older = false; // woundせずにWait

    if(!acquired){
      // 自分が最初のwaiterになる → 現在の所有者(複数のことがある)のwaiter_count_を上げる
      for (uint32_t i = 0; i < TotalThreadNum; i++) {
        if (static_cast<int>(i) == thid_) continue;
        if (tuple->owners[i] != -1) { fprintf(stderr, "[WC+] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_add(1, memory_order_acq_rel)+1); }
      }
      this->wait_entry.insertInto(tuple, local_timestamp);
    }

  }else if(tuple->waiters_head->ts > this->local_timestamp){
    if (wcounter == 0) {
      tuple->owners[thid_] = local_timestamp;
      wcounter = -1;
      acquired = true;
      this->waiter_count_.fetch_add(1, memory_order_acq_rel);
      tuple->owner_older = true;

    }else if(wcounter == -1){
      LockResult woundresult = wound_writelock(tuple);
      if (woundresult == LockResult::ABORTED) {
        tuple->lock_.latch_unlock(wcounter);
        return Status::ERROR_LOCK_FAILED;

      }else if(woundresult == LockResult::SUCCESS) {
        tuple->owners[thid_] = local_timestamp;
        wcounter = -1;
        acquired = true;
        this->waiter_count_.fetch_add(1, memory_order_acq_rel);
      }
      tuple->owner_older = true; // SUCCESS/FAILEDいずれでも確定(既存headのため)

    }else{ // wcounter >= 1
      wcounter = wound_readlock(tuple, wcounter);
      tuple->owner_older = true;
    }

    if (!acquired) this->wait_entry.insertInto(tuple, local_timestamp);

  }else this->wait_entry.insertInto(tuple, local_timestamp);// owner_olderは既存headのための値のまま

  tuple->lock_.latch_unlock(wcounter);

  if (!acquired) {
    // wound_readlockは自分がabortされたことを戻り値では区別できないため,ここで確認する
    if (this->status_ == TransactionStatus::aborted) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return Status::ERROR_LOCK_FAILED;
    }

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
      if (tuple->waiters_head == nullptr) {
        for (uint32_t i = 0; i < TotalThreadNum; i++) {
          if (static_cast<int>(i) == thid_) continue;
          if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
        }
      }
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    if (tuple->waiters_head != &this->wait_entry) continue;

    // これ以降はheadの操作
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
        if (tuple->waiters_head != nullptr) {
          this->waiter_count_.fetch_add(1, memory_order_acq_rel);
          if (result == 0) tuple->owner_older = true; // 自分が最初の一人なら、次のheadより自分は必ず古い
        }
		    result ++;
        this->wait_entry.removeFrom(tuple);
		    tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else{
        tuple->lock_.latch_unlock(result);
      }

    }else if(!tuple->owner_older && (this->waiter_count_.load() > 0 || this->wait_entry.next != nullptr)){
      // expected==-1 and 自分も誰かに待たれる側 → 再度woundを試みる
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

      if(result == -1){
        LockResult woundresult = wound_writelock(tuple);
        if(woundresult == LockResult::ABORTED) {
          this->wait_entry.removeFrom(tuple);
          if (tuple->waiters_head == nullptr) {
            for (uint32_t i = 0; i < TotalThreadNum; i++) {
              if (static_cast<int>(i) == thid_) continue;
              if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
            }
          }
          tuple->lock_.latch_unlock(result);
          return LockResult::ABORTED;
        }else if(woundresult == LockResult::SUCCESS) {
          tuple->owners[thid_] = local_timestamp;
          result = 1;
          this->wait_entry.removeFrom(tuple);

          if (tuple->waiters_head != nullptr) {
            this->waiter_count_.fetch_add(1, memory_order_acq_rel);
            tuple->owner_older = true; // 次のheadに対しても自分(新owner)は必ず古い
          }
          
          tuple->lock_.latch_unlock(result);
          return LockResult::SUCCESS;
        } else { // FAILED: 所有者は自分より古いと確定. 以後このwaitでは再試行しない
          tuple->owner_older = true;
          tuple->lock_.latch_unlock(result);
        }
      }else{ // result >= 0 → ここで直接取得
        tuple->owners[thid_] = local_timestamp;
        result++;
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }
    }
  }
}

LockResult TxExecutor::wait_writeop(Tuple* tuple) {
	while(true){

    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) {
        for (uint32_t i = 0; i < TotalThreadNum; i++) {
          if (static_cast<int>(i) == thid_) continue;
          if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
        }
      }
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    if (tuple->waiters_head != &this->wait_entry) continue;

    // headの操作
    int expected = tuple->lock_.counter.load(memory_order_acquire);

    if (expected == 0) {
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
        tuple->owners[thid_] = local_timestamp;
        result = -1;
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
        tuple->owner_older = true; 
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else tuple->lock_.latch_unlock(result);

    }else if(!tuple->owner_older && (this->waiter_count_.load() > 0 || this->wait_entry.next != nullptr)){
      //headがownersよりもtimestampが大きいという保証がない and サイクルの最小値になりうる

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
        tuple->owners[thid_] = local_timestamp;
        result = -1;
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
        tuple->owner_older = true; 
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;

      }else if (result == -1) {
        LockResult woundresult = wound_writelock(tuple);

        if (woundresult == LockResult::ABORTED) {
          this->wait_entry.removeFrom(tuple);
          if (tuple->waiters_head == nullptr) {
            for (uint32_t i = 0; i < TotalThreadNum; i++) {
              if (static_cast<int>(i) == thid_) continue;
              if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
            }
          }
          tuple->lock_.latch_unlock(result);
          return LockResult::ABORTED;
        }else if(woundresult == LockResult::SUCCESS){
          tuple->owners[thid_] = local_timestamp;
          result = -1;
          this->wait_entry.removeFrom(tuple);
          if(tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
          tuple->owner_older = true;
          tuple->lock_.latch_unlock(result);
          return LockResult::SUCCESS;
        }else if(woundresult == LockResult::FAILED){
          tuple->owner_older = true;
          tuple->lock_.latch_unlock(result);
        }

      }else if(result >= 1){
        result = wound_readlock(tuple, result);
        tuple->owner_older = true;

        if(result == 0){
          tuple->owners[thid_] = local_timestamp;
          result = -1;
          this->wait_entry.removeFrom(tuple);
          if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
          tuple->lock_.latch_unlock(result);
          return LockResult::SUCCESS;
        }else tuple->lock_.latch_unlock(result);
      }
    }
  }
}

LockResult TxExecutor::wait_upgradeop(Tuple* tuple) {
	while(true){

    if (this->status_.load() == TransactionStatus::aborted){
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      if (tuple->waiters_head == nullptr) {
        for (uint32_t i = 0; i < TotalThreadNum; i++) {
          if (static_cast<int>(i) == thid_) continue;
          if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
        }
      }
      tuple->lock_.latch_unlock(r);
      return LockResult::ABORTED;
    }

    if(tuple->delete_flag == true) {
      int r = tuple->lock_.latch_lock();
      this->wait_entry.removeFrom(tuple);
      tuple->lock_.latch_unlock(r);
      return LockResult::NOT_FOUND;
    }

    if (tuple->waiters_head != &this->wait_entry) continue;

    // headの操作
    int expected = tuple->lock_.counter.load(memory_order_acquire);
    if (expected == 1) {

      int result = tuple->lock_.latch_lock();

      if(this->status_ == TransactionStatus::aborted){
        //statusを確認することによって,自分のReadLockが解放されていないことを保証する.
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head == nullptr) {
          for (uint32_t i = 0; i < TotalThreadNum; i++) {
            if (static_cast<int>(i) == thid_) continue;
            if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
          }
        }
        tuple->lock_.latch_unlock(result);
        return LockResult::ABORTED;
      }

      if(tuple->waiters_head != &this->wait_entry){
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result == 1){
        result = -1;
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head != nullptr) this->waiter_count_.fetch_add(1, memory_order_acq_rel);
        tuple->owner_older = true; 
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else tuple->lock_.latch_unlock(result);

    }else if(!tuple->owner_older && (this->waiter_count_.load() > 0 || this->wait_entry.next != nullptr)){
      //expected >= 1の時であり, headのtimestampよりもLockを取得しているTXのtimestampの方が小さいと保証されていない and サイクルの最小値になりうる.
      int result = tuple->lock_.latch_lock();

      if(this->status_ == TransactionStatus::aborted){
        this->wait_entry.removeFrom(tuple);
        if (tuple->waiters_head == nullptr) {
          for (uint32_t i = 0; i < TotalThreadNum; i++) {
            if (static_cast<int>(i) == thid_) continue;
            if (tuple->owners[i] != -1) { fprintf(stderr, "[WC-] src_thid=%d target_thid=%d tuple=%p new=%d\n", thid_, i, (void*)tuple, AllExecutors[i]->waiter_count_.fetch_sub(1, memory_order_acq_rel)-1); }
          }
        }
        tuple->lock_.latch_unlock(result);
        return LockResult::ABORTED;
      }

      if(tuple->waiters_head != &this->wait_entry){
        tuple->lock_.latch_unlock(result);
        continue;
      }

      if(result == 1){ 
        result = -1;
        tuple->owner_older = true; 
        this->wait_entry.removeFrom(tuple);
        tuple->lock_.latch_unlock(result);
        return LockResult::SUCCESS;
      }else if(result > 1){
        result = wound_readlock(tuple, result); 
        tuple->owner_older = true;
        if(result == 1){
          result = -1;
          this->wait_entry.removeFrom(tuple);
          tuple->lock_.latch_unlock(result);
          return LockResult::SUCCESS;
        }else tuple->lock_.latch_unlock(result);
      }
    }
  }
}

LockResult TxExecutor::wound_writelock(Tuple *tuple) {
	for(uint32_t i = 0; i < TotalThreadNum; i++){
    if(tuple->owners[i] == -1){
      continue;
    }else if(tuple->owners[i] > local_timestamp){
      // counterとownersはlatchよってatomicに処理される.そのため,それらが整合していることは保証される.
      // counter = -1ならownersにtimestampを登録しているのは一つのTXしかない.よって,hitすると即座に終了して残りのownersを探索する必要がない.

      TransactionStatus expected = TransactionStatus::inflight;
      if (!AllExecutors[i]->status_.compare_exchange_strong(expected, TransactionStatus::aborted,memory_order_acq_rel, memory_order_acquire)) {
        if(expected == TransactionStatus::aborted){
          tuple->owners[i] = -1; //ownersには値が登録されている.ということはLockもまだ解放されていないということが言える.
          return LockResult::SUCCESS;
        }
        this->status_.store(TransactionStatus::aborted, memory_order_release);
        return LockResult::ABORTED;
      }else{// CASに成功!!
        tuple->owners[i] = -1;
        return LockResult::SUCCESS;
      }
      
    }else if(tuple->owners[i] < local_timestamp) return LockResult::FAILED;
  }

  assert(false && "wound_writelock: counter==-1なのにownersに該当者がいない");
  return LockResult::FAILED;
}

int TxExecutor::wound_readlock(Tuple *tuple, int counter) {
	for (uint32_t i = 0; i < TotalThreadNum; i++) {
	  if(tuple->owners[i] == -1 || tuple->owners[i] == local_timestamp){ //自分をwoundしないため
      continue;
    }else if(tuple->owners[i] > local_timestamp){
      TransactionStatus expected = TransactionStatus::inflight;
      if (!AllExecutors[i]->status_.compare_exchange_strong(expected, TransactionStatus::aborted,memory_order_acq_rel, memory_order_acquire)){
        if(expected == TransactionStatus::aborted){
          tuple->owners[i] = -1;
          counter --;
          continue;
        }
        this->status_.store(TransactionStatus::aborted, memory_order_release);
        return counter;

      }else{ //CAS成功!!
        tuple->owners[i] = -1;
        counter --;
      }
    }
  }
  return counter;
}
