#pragma once

#include <atomic>
#include <mutex>

#include "../../../include/cache_line_size.hh"
#include "../../../include/inline.hh"
#include "../../../include/rwlock.hh"
#include "../../../include/tuple_body.hh"
#include "waitentry.hh" // WaitEntry(宣言だけの insertInto/removeFrom 込み)が使えるようになる
#include "latch_rwlock.hh"

using namespace std;

class Tuple {
public:
  alignas(CACHE_LINE_SIZE) LatchableRWLock lock_;
  TupleBody body_;
  WaitEntry* waiters_head = nullptr;
  bool delete_flag = false;
  int owners[64]; //thread数によっては変更する必要がある. 

  Tuple() {
    for (int i = 0; i < 64; ++i) owners[i] = -1;
  }

  //ベンチマーク開始前の初期データ投入（DBの一括構築）時
  void init([[maybe_unused]] size_t thid, TupleBody&& body,
            [[maybe_unused]] void* p) {
    body_ = std::move(body);
  }
  // insert()から呼ばれる
  void init(TupleBody&& body) {
    body_ = std::move(body);
    lock_.w_lock();
  }
};

inline void WaitEntry::insertInto(Tuple* tuple, int my_ts) {
  this->ts = my_ts;
  this->owner_tuple = tuple;
  WaitEntry* current = tuple->waiters_head;
  WaitEntry* prev_entry = nullptr;

  while (current != nullptr && my_ts > current->ts) {
    prev_entry = current;
    current = current->next;
  }
  this->next = current;
  this->prev = prev_entry;

  if (current != nullptr) current->prev = this;
  if (prev_entry != nullptr) prev_entry->next = this;
  else tuple->waiters_head = this;
}

inline void WaitEntry::removeFrom(Tuple* tuple) {
  if (this->prev != nullptr) this->prev->next = this->next;
  else tuple->waiters_head = this->next;
  if (this->next != nullptr) this->next->prev = this->prev;
  this->next = nullptr;
  this->prev = nullptr;

  this->owner_tuple = nullptr;
}