#include "src/redis_hashes.h"

#include <memory>
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#include "blackwidow/util.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/strings_value_format.h"
#include "src/redis_strings.h"

namespace blackwidow {

//start ibn
    Status RedisHashes::BNHMinOrMax(const Slice &key, const Slice &field,
                                    int64_t value, int32_t *ret, bool is_min) {
      rocksdb::WriteBatch batch;
      ScopeRecordLock l(lock_mgr_, key);

      int32_t version = 0;
      uint32_t statistic = 0;
      std::string meta_value;
      Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
      if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.IsStale()
            || parsed_hashes_meta_value.count() == 0) {
          version = parsed_hashes_meta_value.InitialMetaValue();
          parsed_hashes_meta_value.set_count(1);
          batch.Put(handles_[0], key, meta_value);
          HashesDataKey hashes_data_key(key, version, field);

          char buf[32];
          Int64ToStr(buf, 32, value);
          batch.Put(handles_[1], hashes_data_key.Encode(), buf);
          *ret = 1;
        } else {
          version = parsed_hashes_meta_value.version();
          HashesDataKey hashes_data_key(key, version, field);
          std::string data_value;
          s = db_->Get(default_read_options_, handles_[1],
                       hashes_data_key.Encode(), &data_value);
          if (s.ok()) {
            int64_t ival = 0;
            if (!StrToInt64(data_value.data(), data_value.size(), &ival)) {
              return Status::Corruption("hash value is not an integer");
            }

            if (value == ival) {
              *ret = 1;
            } else if ((is_min && value < ival) || (!is_min && value > ival)) {
              char buf[32];
              Int64ToStr(buf, 32, value);
              batch.Put(handles_[1], hashes_data_key.Encode(), buf);

              statistic++;
              *ret = 1;
            } else {
              *ret = 0;
            }
          } else if (s.IsNotFound()) {
            parsed_hashes_meta_value.ModifyCount(1);
            batch.Put(handles_[0], key, meta_value);

            char buf[32];
            Int64ToStr(buf, 32, value);
            batch.Put(handles_[1], hashes_data_key.Encode(), buf);
            *ret = 1;
          } else {
            return s;
          }
        }
      } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
        version = hashes_meta_value.UpdateVersion();
        batch.Put(handles_[0], key, hashes_meta_value.Encode());
        HashesDataKey hashes_data_key(key, version, field);

        char buf[32];
        Int64ToStr(buf, 32, value);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
        *ret = 1;
      } else {
        return s;
      }
      s = db_->Write(default_write_options_, &batch);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
      return s;
    }

    LockMgr* BNHTLockMgr() {
      return lock_mgr_;
    }

    Status RedisHashes::BNHTSetInternal(const Slice& key, const Slice& field,
                                        int64_t value, int64_t* res) {
      *res = 0; //0=默认值；-1=新值；-2=值相同；正数=老时间戳
      rocksdb::WriteBatch batch;
//      ScopeRecordLock l(lock_mgr_, key);

      int32_t version = 0;
      uint32_t statistic = 0;
      std::string meta_value;
      Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
      if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.IsStale()
            || parsed_hashes_meta_value.count() == 0) {
          version = parsed_hashes_meta_value.InitialMetaValue();
          parsed_hashes_meta_value.set_count(1);
          batch.Put(handles_[0], key, meta_value);
          HashesDataKey data_key(key, version, field);
          char buf[32];
          Int64ToStr(buf, 32, value);
          batch.Put(handles_[1], data_key.Encode(), buf);
          *res = -1;
        } else {
          version = parsed_hashes_meta_value.version();
          std::string data_value;
          HashesDataKey hashes_data_key(key, version, field);
          s = db_->Get(default_read_options_,
                       handles_[1], hashes_data_key.Encode(), &data_value);
          if (s.ok()) {
            int64_t ival = 0;
            if (!StrToInt64(data_value.data(), data_value.size(), &ival)) {
              return Status::Corruption("hash value is not an integer");
            }

            if (value == ival) {
              *res = -2;
              return Status::OK();
            } else {
              char buf[32];
              Int64ToStr(buf, 32, value);
              batch.Put(handles_[1], hashes_data_key.Encode(), buf);
              *res = ival;
              statistic++;
            }
          } else if (s.IsNotFound()) {
            parsed_hashes_meta_value.ModifyCount(1);
            batch.Put(handles_[0], key, meta_value);
            char buf[32];
            Int64ToStr(buf, 32, value);
            batch.Put(handles_[1], hashes_data_key.Encode(), buf);
            *res = -1;
          } else {
            return s;
          }
        }
      } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue meta_value(std::string(str, sizeof(int32_t)));
        version = meta_value.UpdateVersion();
        batch.Put(handles_[0], key, meta_value.Encode());
        HashesDataKey data_key(key, version, field);
        char buf[32];
        Int64ToStr(buf, 32, value);
        batch.Put(handles_[1], data_key.Encode(), buf);
        *res = -1;
      } else {
        return s;
      }
      s = db_->Write(default_write_options_, &batch);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
      return s;
    }
//end ibn

}  //  namespace blackwidow
