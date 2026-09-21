#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <set>

#include "config.hh"
#include "debug.hh"
#include "heap_object.hh"
#include "procedure.hh"
#include "random.hh"
#include "result.hh"
#include "tuple_body.hh"
#include "util.hh"
#include "workload.hh"
#include "zipf.hh"

#include "./tpcc/tpcc_initializer.hh"
#include "./tpcc/tpcc_query.hh"
#include "./tpcc/tpcc_tables.hh"
#include "./tpcc/tpcc_tx_neworder.hh"
#include "./tpcc/tpcc_tx_payment.hh"
#include "./tpcc/tpcc_tx_orderstatus.hh"
#include "./tpcc/tpcc_tx_delivery.hh"
#include "./tpcc/tpcc_tx_stocklevel.hh"

template <typename Tuple, typename Param>
class TPCCWorkload {
public:
  Param* param_;
  Xoroshiro128Plus rnd_;
  HistoryKeyGenerator hkg_;
  uint16_t w_id; // home warehouse

  TPCCWorkload() { rnd_.init(); }

  void prepare(TxExecutor& tx, [[maybe_unused]] Param* p) {
    hkg_.init(tx.thid_, true);
    w_id = (tx.thid_ % FLAGS_tpcc_num_wh) + 1; // home warehouse.
  }

  template <typename TxExecutor, typename TransactionStatus>
  void run(TxExecutor& tx) {
    Query query;
    TPCCQuery::Option option;

  NEWQUERY:
    query.generate(w_id, option);

  RETRY:
    if (tx.isLeader()) { tx.leaderWork(); }

    if (loadAcquire(tx.quit_)) return;

    tx.begin();

    bool op_ok = true;
    switch (query.type) {
      case TxType::NewOrder:
        op_ok =
            run_new_order<TxExecutor, TransactionStatus>(tx, &query.new_order);
        break;
      case TxType::Payment:
        op_ok = run_payment<TxExecutor, TransactionStatus, Tuple>(
            tx, &query.payment, &hkg_);
        break;
      case TxType::OrderStatus:
        op_ok = run_order_status<TxExecutor, TransactionStatus, Tuple>(
            tx, &query.order_status);
        break;
      case TxType::Delivery:
        op_ok = run_delivery<TxExecutor, TransactionStatus>(tx, &query.delivery);
        break;
      case TxType::StockLevel:
        op_ok = run_stock_level<TxExecutor, TransactionStatus>(
            tx, &query.stock_level);
        break;
      default:
        ERR;
        break;
    }

    /* run_*() が false を返す理由は 2 通りある。他スレッドに wound されて
     * status_ が aborted になった場合と、操作が Status::OK 以外 (WARN_NOT_FOUND
     * など) を返した場合。後者では status_ はまだ inflight なので、ここで
     * aborted へ上書きする前に区別しておく。 */
    const bool wounded = (tx.status_ == TransactionStatus::aborted);
    if (!op_ok) tx.status_ = TransactionStatus::aborted;

    if (tx.status_ == TransactionStatus::aborted) {
      tx.abort();
      tx.result_->local_abort_counts_++;
      tx.result_->local_abort_counts_per_tx_[get_tx_type(query.type)]++;
      if (wounded) {
        tx.result_->local_abort_by_wound_per_tx_[get_tx_type(query.type)]++;
      } else {
        tx.result_->local_abort_by_status_per_tx_[get_tx_type(query.type)]++;
      }
#if ADD_ANALYSIS
      ++tx.result_->local_early_aborts_;
#endif
      /* rbk==1 の NewOrder は generate() が存在しない商品番号を作るので
       * (tpcc_query.hh: items[ol_cnt-1].ol_i_id += max_items)、再実行しても
       * Item テーブルは読み取り専用で行が増えないため永遠に失敗する。
       * TPC-C 仕様の 1% intentional rollback、すなわちユーザ起因の中断なので
       * 新しい query を引く。DBx1000 の RC=ERROR / ERMIA の RC_ABORT_USER を
       * retry 対象から外しているのと同じ扱い。
       * query は union なので、type の判定を短絡評価で先に置く必要がある。 */
      if (query.type == TxType::NewOrder && query.new_order.rbk == 1) goto NEWQUERY;
      goto RETRY;
    }

    if (!tx.commit()) {
      tx.abort();
      if (tx.status_ == TransactionStatus::invalid) return;
      tx.result_->local_abort_counts_++;
      tx.result_->local_abort_counts_per_tx_[get_tx_type(query.type)]++;
      /* commit() は status_ を inflight から committed へ CAS するだけなので、
       * 失敗するのは他スレッドが aborted を書き込んだ場合に限られる。 */
      tx.result_->local_abort_by_wound_per_tx_[get_tx_type(query.type)]++;
      goto RETRY;
    }

    if (loadAcquire(tx.quit_)) return;
    tx.result_->local_commit_counts_++;
    tx.result_->local_commit_counts_per_tx_[get_tx_type(query.type)]++;

    return;
  }

  static uint32_t getTableNum() { return (uint32_t) Storage::Size; }

  static void makeDB([[maybe_unused]] Param* param) {
    Xoroshiro128Plus rand;
    rand.init();

    // TODO: move this codes to appropriate place
    set_tx_name(TxType::NewOrder, "NewOrder");
    set_tx_name(TxType::Payment, "Payment");
    set_tx_name(TxType::OrderStatus, "OrderStatus");
    set_tx_name(TxType::Delivery, "Delivery");
    set_tx_name(TxType::StockLevel, "StockLevel");

    TPCCInitializaer<Tuple, Param>::load(param);
  }

  static void displayWorkloadParameter() {}

  static void displayWorkloadResult() {}
};
