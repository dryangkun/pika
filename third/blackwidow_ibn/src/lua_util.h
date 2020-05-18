//
// Created by dryoung on 2020/5/18.
//

#ifndef PIKA_LUA_UTIL_H
#define PIKA_LUA_UTIL_H

#include <string>
#include <thread>
#include <unordered_map>

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include "blackwidow/blackwidow.h"
#include "slash/include/slash_mutex.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"

#include "src/redis_hashes.h"

namespace blackwidow {

typedef std::pair<rocksdb::Status, std::string> LuaUtilPair;
const std::string LuaUtilObjStr = "LuaUtilObj";

class LuaUtil {
public:
  LuaUtil();

  ~LuaUtil();

  rocksdb::Status ScriptSet(RedisHashes *hashes_db_, DataType type, const rocksdb::Slice &luaKey,
                            const Slice &value, int32_t* res);

  rocksdb::Status ScriptGet(RedisHashes *hashes_db_, DataType type, const rocksdb::Slice &luaKey, std::string *value);

  lua_State* StateOpen();

private:
  std::unordered_map<std::thread::id, lua_State *> luaState_map_;
  slash::Mutex luaState_mutex_;
  std::unordered_map <std::string, std::string> luaScript_map_;
  slash::Mutex luaScript_mutex_;
};

}


#endif //PIKA_LUA_UTIL_H
