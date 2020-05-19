#ifndef PIKA_IBN_H_
#define PIKA_IBN_H_

#include "include/pika_command.h"
#include "blackwidow/blackwidow.h"


/*
 * hash ibn
 */

class BNHMinCmd : public Cmd {
public:
  BNHMinCmd() {}

  virtual void Do();

private:
  std::string key_, field_;
  int64_t value_;

  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo *const ptr_info);
};

class BNHMaxCmd : public Cmd {
public:
  BNHMaxCmd() {}

  virtual void Do();

private:
  std::string key_, field_;
  int64_t value_;

  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo *const ptr_info);
};

class BNHistoryRangeCmd : public Cmd {
public:
  BNHistoryRangeCmd() {}

  virtual void Do();

private:
  std::string key_;
  std::vector <std::string> fields;
  int64_t value_, r_val_;

  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo *const ptr_info);
};

class BNStreamCmd : public Cmd {
public:
  BNStreamCmd() {};

  virtual void Do();

private:
  std::string key_;
  std::vector <std::string> values_;

  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo *const ptr_info);

  virtual void Clear() {
    values_.clear();
  }
};

class BNMSetexCmd : public Cmd {
public:
  BNMSetexCmd() {}

  virtual void Do();

private:
  std::vector <blackwidow::KeyValue> kvs_;
  int64_t sec_;

  virtual void DoInitial(const PikaCmdArgsType &argv, const CmdInfo *const ptr_info);
};

class BNHScriptLoadCmd : public Cmd {
public:
  BNHScriptLoadCmd() {}

  virtual void Do();

private:
  std::string luaKey_, luaScript_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class BNHEvalCmd : public Cmd {
public:
  BNHEvalCmd() {}

  virtual void Do();

private:
  std::string luaKey_, key_;
  std::vector<std::string> args_;
  virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    args_.clear();
  }
};

#endif
