//
// Created by dryoung on 2020/5/18.
//

#include "src/lua_util_hashes.h"

namespace blackwidow {

static int LuaUtilHashesGet(lua_State *L) {
  lua_getglobal(L, LuaUtilObjStr);
  LuaUtilHashes* luaHashes = (LuaUtilHashes *)lua_touserdata(L, -1);
  lua_setglobal(L, LuaUtilObjStr);

  int n = lua_gettop(L);
  if (n != 1) {
    lua_pushnil(L);
    lua_pushstring(L, "parameter number != 1");
    return 2;
  }

  std::string field = LuaUtilToString(L, -1);
  if (!luaHashes) {
    lua_pushnil(L);
    lua_pushstring(L, "global variable LuaUtilObj not found");
    return 2;
  }

  std::string value;
  rocksdb::Status s = luaHashes->Get(field, &value);
  if (s.ok()) {
    lua_pushlstring(L, value.c_str, value.length());
    return 1;
  } else if (s.IsNotFound()) {
    lua_pushboolean(L, 0);
    return 1;
  } else {
    value = s.ToString();
    lua_pushnil(L);
    lua_pushlstring(L, value.c_str(), value.length());
    return 2;
  }
}

static int LuaUtilHashesSet(lua_State *L) {
  lua_getglobal(L, LuaUtilObjStr);
  LuaUtilHashes* luaHashes = (LuaUtilHashes *)lua_touserdata(L, -1);
  lua_setglobal(L, LuaUtilObjStr);

  int n = lua_gettop(L);
  if (n != 2) {
    lua_pushnil(L);
    lua_pushstring(L, "parameter number != 2");
    return 2;
  }

  std::string field = LuaUtilToString(L, -2);
  std::string value = LuaUtilToString(L, -1);
  if (!luaHashes) {
    lua_pushnil(L);
    lua_pushstring(L, "global variable LuaUtilObj not found");
    return 2;
  }

  luaHashes->Set(string(field), string(value));
  return 0;
}

LuaUtilHashes::LuaUtilHashes(RedisHashes *hashes_db_, const Slice &key) {
  this->hashes_db_ = hashes_db_;
  this->key_ = key;
  not_found_ = false;
}

rocksdb::Status LuaUtilHashes::Get(std::string field, std::string *value) {
  if (not_found_) {
    return rocksdb::Status::NotFound("");
  }

  std::map<std::string, LuaUtilPair>::iterator iter = reads_.find(field);
  if (iter != reads.end()) {
    return iter->second.first;
  }

  rocksdb::Status s = hashes_db_->BNInternalHGet(key_, field, value);
  if (s.ok()) {
    LuaUtilPair pair(s, *value);
    reads_[field] = pair;
  } else {
    LuaUtilPair pair(s, "");
    reads_[field] = pair;
  }
  return s;
}

void LuaUtilHashes::Set(std::string field, std::string value) {
  writes_[field] = value;
}

void LuaUtilHashes::LuaRegister(lua_State* L) {
  lua_register(L, "pika_hget", LuaUtilHashesGet);
  lua_register(L, "pika_hset", LuaUtilHashesSet);
}

}