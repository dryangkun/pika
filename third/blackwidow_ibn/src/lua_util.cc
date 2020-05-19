//
// Created by dryoung on 2020/5/18.
//

#include "src/lua_util.h"
#include "src/lua_util_hashes.h"

namespace blackwidow {

std::string LuaUtilToString(lua_State* L, int index) {
  size_t str_len = 0;
  const char* str = lua_tolstring(L, -1, &str_len);
  return std::string(str, str_len);
}

LuaUtil::LuaUtilObjStr = "_LuaUtilObj_";

LuaUtil::LuaUtil() {
}

LuaUtil::~LuaUtil() {
  luaState_mutex_.Lock();
  std::unordered_map<std::thread::id, lua_State *>::iterator iter;
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
  std::unordered_map<std::string, std::string>::iterator iter;
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
  std::unordered_map<std::thread::id, lua_State *>::iterator iter;

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
  LuaUtilHashes::LuaRegister(L);

  luaState_map_[tid] = L;
  luaState_mutex_.Unlock();
  return L;
}

rocksdb::Status LuaUtil::StateExecute(lua_State* L, std::string luaScript, void *obj,
                                      const std::vector<std::string>& args,
                                      std::vector<std::string>* ret) {
  int lua_error = 0;
  std::string lua_msg;
  rocksdb::Status s;
  int num = 0;

  lua_pushlightuserdata(L, obj);
  lua_setglobal(L, LuaUtilObjStr);

  for (const auto& arg : args) {
    lua_pushlstring(L, arg.c_str(), arg.length());
  }
  lua_error = luaL_dostring(L, luaScript.c_str());
  if (lua_error) {
    lua_msg = "luaL_dostring fail - " + std::to_string(lua_error);
    s = rocksdb::Status::InvalidArgument(lua_msg);
    goto end;
  }

  num = lua_gettop(L);
  if (num == 0) {
    s = rocksdb::Status::OK();
    goto end;
  }

  if (lua_isnil(L, -1)) {
    lua_msg = "lua script return nil";
    if (num >= 2 && lua_isstring(L, -2)) {
      lua_msg.append(" - ");
      lua_msg.append(LuaUtilToString(L, -2));
    }
    s = rocksdb::Status::InvalidArgument(lua_msg);
    goto end;
  }

  s = rocksdb::Status::OK();
  if (lua_isstring(L, -1)) {
    //返回string
    ret->push_back(LuaUtilToString(L, -1));
  } else if (lua_isnumber(L, -1)) {
    //返回number
    ret->push_back(std::to_string(lua_tonumber(L, -1)));
  } else if (lua_istable(L, -1)) {
    //返回table
    lua_pushnil(L);
    while (lua_next(L, -2)) {
      if (lua_isnumber(L, -1)) {
        ret->push_back(std::to_string(lua_tonumber(L, -1)));
      } else if (lua_isstring(L, -1)) {
        ret->push_back(LuaUtilToString(L, -1));
      }
      lua_pop(L, 1);
    }
  }

  end:
  lua_settop(L, 0);
  lua_pushnil(L);
  lua_setglobal(L, LuaUtilObjStr);
  return s;
}

}