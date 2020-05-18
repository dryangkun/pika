//
// Created by dryoung on 2020/5/18.
//

#ifndef PIKA_LUA_UTIL_HASHES_H
#define PIKA_LUA_UTIL_HASHES_H

#include <string>
#include <map>

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include "rocksdb/status.h"
#include "rocksdb/slice.h"

#include "src/lua_util.h"

namespace blackwidow {

class LuaUtilHashes {
public:
  RedisHashes *hashes_db_;
  Slice& key_;

  bool not_found_;
  std::map<std::string, LuaUtilPair> reads_;
  std::map<std::string, std::string> writes_;

  LuaUtilHashes(RedisHashes *hashes_db_, const Slice& key);

  rocksdb::Status Get(std::string field, std::string* value);

  void Set(std::string field, std::string value);

  static void LuaRegister(lua_State* L);
};

}


#endif //PIKA_LUA_UTIL_HASHES_H
