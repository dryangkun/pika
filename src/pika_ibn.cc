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

void BNHistoryRangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNHistoryRange);
    return;
  }
  key_ = argv[1];
  fields.clear();
  fields.push_back(argv[3].data());
  fields.push_back(argv[2].data());
  if (!slash::string2l(argv[4].data(), argv[4].size(), &value_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHistoryRange);
    return;
  }
  if (!slash::string2l(argv[5].data(), argv[5].size(), &r_val_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameBNHistoryRange);
    return;
  }
  return;
}

void BNHistoryRangeCmd::Do() {
  int32_t ret = 0;
  rocksdb::Status s = g_pika_server->db()->BNHistoryRange(key_, fields, value_, r_val_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
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
  //key + chr(255) + '0' + 8byte(启动时间) + 16byte(counter)
  //主从切换，写不同的机器，客户端要通过key来保持不一样，scan时要进行多次扫描

  std::string prefix;
  prefix.reserve(key_.size() + 2 + 16);
  prefix.append(key_);
  prefix += (unsigned char) 255;
  prefix += '0';

  slash::ReadLock l(g_pika_server->BNStreamRWLock());

  char id_bytes[17];
  sprintf(id_bytes, "%08"
  PRIx32
  "",
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
    sprintf(counter_buf, "%016"
    PRIx64
    "", g_pika_server->BNStreamCounter());
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

void BNMSetexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
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

void BNHScriptLoadCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNHScriptLoad);
    return;
  }
  luaKey_ = argv[1];
  luaScript_ = argv[2];
  return;
}

void BNHScriptLoadCmd::Do() {
  int32_t ret = 0;
  blackwidow::Status s = g_pika_server->db()->BNHScriptLoad(luaKey_, luaScript_, &ret);
  if (s.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
};

void BNHEvalCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBNHScriptLoad);
    return;
  }
  luaKey_ = argv[1];
  key_ = argv[2];
  size_t pos = 3;
  while (pos < argv.size()) {
    args_.push_back(argv[pos++]);
  }
  return;
}

void BNHEvalCmd::Do() {
  std::vector<std::string> values;
  blackwidow::Status s = g_pika_server->db()->BNHEval(luaKey_, key_, args_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s.IsNotFound()) {
    res_.AppendArrayLen(0);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}