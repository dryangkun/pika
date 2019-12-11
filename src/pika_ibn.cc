#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include <atomic>

#include "slash/include/slash_string.h"
#include "include/pika_ibn.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"

extern PikaConf *g_pika_conf;
extern PikaServer *g_pika_server;
extern PikaClientConn *g_pika_client_conn;

void BNHMinCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNHMin);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  if (!slash::string2l(argv[3].data(), argv[3].size(), &value_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHMin);
    return;
  }
  return;
}

void BNHMinCmd::Do() {
  int32_t ret = 0;
  rocksdb::Status s = g_pika_server->db()->BNHMinOrMax(key_, field_, value_, &ret, true);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void BNHMaxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNHMax);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  if (!slash::string2l(argv[3].data(), argv[3].size(), &value_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHMax);
    return;
  }
  return;
}

void BNHMaxCmd::Do() {
  int32_t ret = 0;
  rocksdb::Status s = g_pika_server->db()->BNHMinOrMax(key_, field_, value_, &ret, false);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

static int32_t bnhtIndexValueTTL = 7 * 86400;
static rocksdb::Slice bnhtIndexValueEmpty = rocksdb::Slice("");

static const rocksdb::Slice bnhtIndexEncode(const rocksdb::Slice &key, int prefix_length, int64_t value) {
  std::string keyStr = key.ToString();
  char valueStr[17];
  sprintf(valueStr, "%016" PRIx64 "", value);

  //".ht" . chr(255) . prefix(key) . chr(255) . hex(value) . chr(255) . suffix(key)
  std::string buf;
  buf.reserve(key.size() + 23);
  buf.append(".ht");
  buf += (unsigned char) 255;
  buf.append(keyStr.substr(0, prefix_length));
  buf += (unsigned char) 255;
  buf.append(valueStr);
  buf += (unsigned char) 255;
  buf.append(keyStr.substr(prefix_length));
  return rocksdb::Slice(buf);
}

void BNHTIndexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNHTIndex);
    return;
  }
  key_ = argv[1];

  field_ = argv[2];
  if (field_.size() < 4) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHTIndex);
    return;
  }
  if (field_.compare(0, 3, "_t|") != 0) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHTIndex);
    return;
  }
  std::string tmp = field_.substr(3);
  char *end = nullptr;
  prefix_length_ = strtol(tmp.c_str(), &end, 10);
  if (*end != 0) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHTIndex);
    return;
  }

  if (!slash::string2l(argv[3].data(), argv[3].size(), &value_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHTIndex);
    return;
  }
  return;
}

void BNHTIndexCmd::Do() {
  int64_t old_value = 0;
  rocksdb::Status s = g_pika_server->db()->BNHTIndexGetSet(key_, field_, value_, &old_value);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  int32_t ret = 0;
  if (old_value == -1) { //新值
    ret = 1;
    const rocksdb::Slice htKey1 = htIndexEncode(key_, prefix_length_, value_);
    g_pika_server->db()->Setex(htKey1, bnhtIndexValueEmpty, bnhtIndexValueTTL);
  } else if (old_value > 0) { //老值
    std::vector <std::string> htKeys;
    const rocksdb::Slice htKey1 = htIndexEncode(key_, prefix_length_, old_value);
    htKeys.push_back(htKey1);
    g_pika_server->db()->DelByType(htKeys, blackwidow::DataType::kStrings);

    const rocksdb::Slice htKey2 = htIndexEncode(key_, prefix_length_, value_);
    g_pika_server->db()->Setex(htKey2, bnhtIndexValueEmpty, bnhtIndexValueTTL);
  }
  res_.AppendContent(":" + std::to_string(ret));
}

static std::string streamValueTTLStr("604800");
static int32_t streamValueTTL = 604800;

void BNStreamCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNStream);
    return;
  }
  key_ = argv[1];
  size_t pos = 2;
  values_.clear();
  while (pos < argv.size()) {
    values_.push_back(argv[pos++]);
  }
}

void BNStreamCmd::Do() {
  //key + chr(255) + '0' + 递增的offset

  std::string prefix;
  prefix.reserve(key_.size() + 2 + 16);
  prefix.append(key_);
  prefix += (unsigned char) 255;
  prefix += '0';

  slash::ReadLock l(g_pika_server->BNStreamRWLock());

  char id_bytes[17];
  sprintf(id_bytes, "%08" PRIx32 "%08" PRIx32 "",
          (uint32_t) std::stoi(g_pika_conf->server_id()),
          g_pika_server->BNStreamOffset());
  prefix.append(id_bytes);

  PikaCmdArgsType *bnmsetex_argv = new PikaCmdArgsType();
  std::vector <blackwidow::KeyValue> bnmsetex_kvs;

  bnmsetex_argv->clear();
  bnmsetex_argv->push_back(kCmdNameBNMSetex);

  bnmsetex_kvs.clear();
  for (const auto &value : values_) {
    std::string key;
    key.append(prefix);
    char counter_buf[17];
    sprintf(counter_buf, "%016" PRIx64 "", g_pika_server->BNStreamCounter());
    key.append(counter_buf);

    bnmsetex_kvs.push_back({key, value});
    bnmsetex_argv->push_back(key);
    bnmsetex_argv->push_back(value);
  }
  bnmsetex_argv->push_back(streamValueTTLStr);

  blackwidow::Status s = g_pika_server->db()->BNMSetex(bnmsetex_kvs, streamValueTTL);
  if (!s.ok()) {
    delete bnmsetex_argv;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  //bnstream写的binlog必须是bnmsetex，不然从库执行bnstream数据就不对
  res_.new_argv = bnmsetex_argv;
  res_.SetRes(CmdRes::kOk);
  return;
}

void BNMSetexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNMSetex);
    return;
  }
  size_t argc = argv.size();
  if (argc % 2 == 1) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNMSetex);
    return;
  }
  kvs_.clear();
  for (size_t index = 1; index < argc - 1; index += 2) {
    kvs_.push_back({argv[index], argv[index + 1]});
  }
  if (!slash::string2l(argv[argc - 1].data(), argv[argc - 1].size(), &sec_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void BNMSetexCmd::Do() {
  blackwidow::Status s = g_pika_server->db()->BNMSetex(kvs_, sec_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}