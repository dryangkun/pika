//
// Created by dryoung on 2020/5/18.
//

#include "src/lua_util.h"

namespace blackwidow {

LuaUtil::LuaUtil() {

}

LuaUtil::~LuaUtil() {
  luaState_mutex_.Lock();
  std::map<std::thread::id, lua_State *>::iterator iter;
  for (iter = luaState_map_.begin(); iter != luaState_map_.end(); iter++) {
    if (iter->second) {
      lua_close(iter->second);
    }
  }
  luaState_map_.clear();
  luaState_mutex_.Unlock();

  luaScript_mutex_.Lock();
  luaScript_map_.clear();
  luaScript_mutex_.Unlock();
}

rocksdb::Status LuaUtil::ScriptSet(RedisHashes *hashes_db_, DataType type, const rocksdb::Slice &luaKey,
                                   const rocksdb::Slice &value, int32_t* res) {
  std::string key = "luascript_save_";
  switch (type) {
    case DataType::kHashes:
      key.append(HASHES_DB);
      break;
    default:
      return rocksdb::Status::NotSupported("");
  }

  luaScript_mutex_.Lock();
  rocksdb::Status s = hashes_db_->HSet(key, luaKey, value, res);
  if (!s.ok()) {
    luaScript_mutex_.Unlock();
    return s;
  }

  key.append(luaKey.ToString());
  luaScript_map_[key] = value.ToString();
  luaScript_mutex_.Unlock();
  return rocksdb::Status::OK();
}

rocksdb::Status LuaUtil::ScriptGet(RedisHashes *hashes_db_, DataType type, const rocksdb::Slice &luaKey, std::string* value) {
  std::string key = "luascript_save_";
  switch (type) {
    case DataType::kHashes:
      key.append(HASHES_DB);
      break;
    default:
      return rocksdb::Status::NotSupported("");
  }

  std::string fieldStr = key + luaKey.ToString();
  std::map<std::string, std::string>::iterator iter;
  iter = luaScript_map_.find(fieldStr);
  if (iter != luaScript_map_.end()) {
    *value = iter->second;
    return rocksdb::Status::OK();
  }

  luaScript_mutex_.Lock();
  iter = luaScript_map_.find(fieldStr);
  if (iter != luaScript_map_.end()) {
    *value = iter->second;
    luaScript_mutex_.Unlock();
    return rocksdb::Status::OK();
  }

  rocksdb::Status s = hashes_db_->HGet(key, luaKey, value);
  if (s.ok()) {
    luaScript_map_[fieldStr] = *value;
  }
  luaScript_mutex_.Unlock();
  return s;
}

lua_State * LuaUtil::StateOpen() {
  std::map<std::thread::id, lua_State *>::iterator iter;

  std::thread::id tid = std::this_thread::get_id();
  iter = luaState_map_.find(tid);
  if (iter != luaState_map_.end()) {
    return iter->second;
  }

  luaState_mutex_.Lock();
  iter = luaState_map_.find(tid);
  if (iter != luaState_map_.end()) {
    luaState_mutex_.Unlock();
    return iter->second;
  }

  lua_State *L = luaL_newstate();
  luaL_openlibs(L);
  luaState_map_[tid] = L;
  luaState_mutex_.Unlock();
  return L;
}

}