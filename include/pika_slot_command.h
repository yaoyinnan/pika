#ifndef PIKA_SLOT_COMMAND_H_
#define PIKA_SLOT_COMMAND_H_

#include "include/pika_client_conn.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "net/include/net_cli.h"
#include "net/include/net_thread.h"
#include "storage/storage.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";
const std::string SlotTagPrefix = "_internal:slottag:4migrate:";

const size_t MaxKeySendSize = 10 * 1024;

extern uint32_t crc32tab[256];

void CRC32TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char *buf, int32_t len);
extern uint32_t CRC32CheckSum(const char *buf, int32_t len);

int32_t GetSlotID(const std::string &str);
int32_t GetKeyType(const std::string& key, std::string &key_type, const std::shared_ptr<Slot>& slot);
void AddSlotKey(const std::string& type, const std::string& key, const std::shared_ptr<Slot>& slot);
void RemSlotKey(const std::string& key, const std::shared_ptr<Slot>& slot);
int32_t DeleteKey(const std::string& key, const char key_type, const std::shared_ptr<Slot>& slot);
std::string GetSlotKey(int32_t slot);
std::string GetSlotsTagKey(uint32_t crc);
int32_t GetSlotsID(const std::string &str, uint32_t *pcrc, int32_t *phastag);
void RemSlotKeyByType(const std::string &type, const std::string &key, const std::shared_ptr<Slot>& slot);

class PikaMigrate {
 public:
  PikaMigrate();
  virtual ~PikaMigrate();

  int32_t MigrateKey(const std::string &host, const int32_t port, int32_t timeout, const std::string &key, const char type,
                 std::string &detail, const std::shared_ptr<Slot>& slot);
  void CleanMigrateClient();

  void Lock() {
    mutex_.lock();
  }
  int32_t Trylock() {
    return mutex_.try_lock();
  }
  void Unlock() {
    mutex_.unlock();
  }
  net::NetCli *GetMigrateClient(const std::string &host, const int32_t port, int32_t timeout);

 private:
  std::map<std::string, void *> migrate_clients_;
  pstd::Mutex mutex_;

  void KillMigrateClient(net::NetCli *migrate_cli);
  void KillAllMigrateClient();

  int32_t MigrateSend(net::NetCli *migrate_cli, const std::string &key, const char type, std::string &detail,
                  const std::shared_ptr<Slot>& slot);
  bool MigrateRecv(net::NetCli *migrate_cli, int32_t need_receive, std::string &detail);

  int32_t ParseKey(const std::string &key, const char type, std::string &wbuf_str, const std::shared_ptr<Slot>& slot);
  int64_t TTLByType(const char key_type, const std::string &key, const std::shared_ptr<Slot>& slot);
  int32_t ParseKKey(const std::string &key, std::string &wbuf_str, const std::shared_ptr<Slot>& slot);
  int32_t ParseZKey(const std::string &key, std::string &wbuf_str, const std::shared_ptr<Slot>& slot);
  int32_t ParseSKey(const std::string &key, std::string &wbuf_str, const std::shared_ptr<Slot>& slot);
  int32_t ParseHKey(const std::string &key, std::string &wbuf_str, const std::shared_ptr<Slot>& slot);
  int32_t ParseLKey(const std::string &key, std::string &wbuf_str, const std::shared_ptr<Slot>& slot);
  bool SetTTL(const std::string &key, std::string &wbuf_str, int64_t ttl);
};

class SlotsMgrtTagSlotCmd : public Cmd {
 public:
  SlotsMgrtTagSlotCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagSlotCmd(*this); }
 private:
  std::string dest_ip_;
  int64_t dest_port_ = 0;
  int64_t timeout_ms_ = 60;
  int64_t slot_id_ = 0;
  std::basic_string<char, std::char_traits<char>, std::allocator<char>> key_;

  void DoInitial() override;
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtTagSlotAsyncCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag){}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override{};
  Cmd* Clone() override { return new SlotsMgrtTagSlotAsyncCmd(*this); }
 private:
  std::string dest_ip_;
  int64_t dest_port_ = 0;
  int64_t timeout_ms_ = 60;
  int64_t max_bulks_ = 0;
  int64_t max_bytes_ = 0;
  int64_t slot_id_ = 0;
  int64_t keys_num_ = 0;

  void DoInitial() override;
};

class SlotsMgrtTagOneCmd : public Cmd {
 public:
  SlotsMgrtTagOneCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtTagOneCmd(*this); }
 private:
  std::string dest_ip_;
  int64_t dest_port_ = 0;
  int64_t timeout_ms_ = 60;
  std::string key_;
  int64_t slot_id_ = 0;
  char key_type_ = '\0';
  void DoInitial() override;
  int32_t KeyTypeCheck(const std::shared_ptr<Slot>& slot);
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
 public:
  SlotsMgrtAsyncStatusCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtAsyncStatusCmd(*this); }

 private:
  void DoInitial() override;
};

class SlotsInfoCmd : public Cmd {
 public:
  SlotsInfoCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsInfoCmd(*this); }
 private:
  void DoInitial() override;

  int64_t begin_ = 0;
  int64_t end_ = 1024;
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
 public:
  SlotsMgrtAsyncCancelCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtAsyncCancelCmd(*this); }
 private:
  void DoInitial() override;
};

class SlotsDelCmd : public Cmd {
 public:
  SlotsDelCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsDelCmd(*this); }
 private:
  std::vector<std::string> slots_;
  void DoInitial() override;
};

class SlotsHashKeyCmd : public Cmd {
 public:
  SlotsHashKeyCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsHashKeyCmd(*this); }
 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
};

class SlotsScanCmd : public Cmd {
 public:
  SlotsScanCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsScanCmd(*this); }
 private:
  std::string key_;
  std::string pattern_ = "*";
  int64_t cursor_ = 0;
  int64_t count_ = 10;
  void DoInitial()  override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
};

/* *
* SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$arg1 ...]
* SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$key1 $arg1 ...]
* SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$key1 $arg1 ...] [$key2 $arg2 ...]
* */
class SlotsMgrtExecWrapperCmd : public Cmd {
 public:
  SlotsMgrtExecWrapperCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsMgrtExecWrapperCmd(*this); }
 private:
  std::string key_;
  std::vector<std::string> args;
  void DoInitial() override;
};


class SlotsReloadCmd : public Cmd {
 public:
  SlotsReloadCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsReloadCmd(*this); }
 private:
  void DoInitial() override;
};

class SlotsReloadOffCmd : public Cmd {
 public:
  SlotsReloadOffCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot>slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsReloadOffCmd(*this); }
 private:
  void DoInitial() override;
};

class SlotsCleanupCmd : public Cmd {
 public:
  SlotsCleanupCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsCleanupCmd(*this); }
  std::vector<int32_t> cleanup_slots_;
 private:
  void DoInitial() override;
};

class SlotsCleanupOffCmd : public Cmd {
 public:
  SlotsCleanupOffCmd(const std::string& name, int32_t arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SlotsCleanupOffCmd(*this); }
 private:
  void DoInitial() override;
};

#endif
