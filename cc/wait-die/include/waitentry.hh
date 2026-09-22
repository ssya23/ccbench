#pragma once

#include <atomic>

class Tuple;  // 前方宣言のみ。本体はtuple.hhの末尾(Tuple定義の後)に置く。

struct WaitEntry { // WaitListの1エントリ。
  std::atomic<bool> is_head{false};
  int ts;
  WaitEntry* next = nullptr;
  WaitEntry* prev = nullptr;

  void insertInto(Tuple* tuple, int my_ts); // 自分のWaitEntryをWaitListにSortされた順で挿入する
  void removeFrom(Tuple* tuple);// WaitListから外す
};