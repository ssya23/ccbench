#pragma once

#include <atomic>

class Tuple;  // 前方宣言のみ。本体はtuple.hhの末尾(Tuple定義の後)に置く。

// woundするかどうかの優先度判定に使う、待ち行列(wait list)の1エントリ。
// この構造体自体はPODで、スレッド間のアクセスはタプル側のlatch
// (LatchableRWLock::latch_lock()/latch_unlock()) で排他されている前提。
struct WaitEntry {
  std::atomic<bool> is_head{false};
  int ts;
  WaitEntry* next = nullptr;
  WaitEntry* prev = nullptr;

  // 自分自身を、tsの昇順でtupleの待ち行列に挿入する。
  // 呼び出し側はtupleのlatchを保持していること。
  void insertInto(Tuple* tuple, int my_ts);

  // 自分自身を、tupleの待ち行列から外す。
  // 呼び出し側はtupleのlatchを保持していること。
  void removeFrom(Tuple* tuple);
};