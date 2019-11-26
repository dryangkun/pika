//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_strings.h"

#include <memory>
#include <climits>
#include <algorithm>
#include <limits>

#include "blackwidow/util.h"
#include "src/strings_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

    Status RedisStrings::BNMSetex(const std::vector <KeyValue> &kvs, int32_t ttl) {
      std::vector <std::string> keys;
      for (const auto &kv :  kvs) {
        keys.push_back(kv.key);
      }

      MultiScopeRecordLock ml(lock_mgr_, keys);
      rocksdb::WriteBatch batch;
      for (const auto &kv : kvs) {
        StringsValue strings_value(kv.value);
        strings_value.SetRelativeTimestamp(ttl);
        batch.Put(kv.key, strings_value.Encode());
      }
      return db_->Write(default_write_options_, &batch);
    }

}  //  namespace blackwidow
