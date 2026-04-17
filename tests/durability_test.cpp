#include "log_storage/durability/durability_mode.hpp"
#include "log_storage/reader/log_reader.hpp"
#include "log_storage/writer/durable_log_writer.hpp"

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using log_storage::DurableLogWriter;
using log_storage::DurabilityManager;
using log_storage::DurabilityMode;
using log_storage::LogReader;

std::string make_tmp_wal() {
  char tmpl[] = "/tmp/dur_test_XXXXXX";
  const int fd = ::mkstemp(tmpl);
  if (fd < 0) {
    throw std::runtime_error("mkstemp");
  }
  ::close(fd);
  return std::string(tmpl);
}

void test_sync_readback() {
  const std::string path = make_tmp_wal();
  DurabilityManager::Config cfg;
  cfg.mode = DurabilityMode::Sync;
  cfg.sync_batch_max_records = 1;
  cfg.sync_batch_interval_ms = 99999.0;
  {
    DurableLogWriter w(path, cfg);
    w.append(std::vector<std::uint8_t>{'d', 'a', 't', 'a'});
    if (w.fsync_count() < 1u) {
      throw std::runtime_error("expected at least one fsync in SYNC");
    }
  }
  LogReader r(path);
  std::vector<std::uint8_t> pl;
  std::vector<std::uint8_t> last;
  std::uint64_t n = 0;
  while (r.read_next(pl)) {
    last = pl;
    ++n;
  }
  if (n != 1u || last != std::vector<std::uint8_t>{'d', 'a', 't', 'a'}) {
    throw std::runtime_error("sync durability readback failed");
  }
  ::unlink(path.c_str());
}

void test_group_commit_fewer_fsync_than_appends() {
  const std::string path = make_tmp_wal();
  DurabilityManager::Config cfg;
  cfg.mode = DurabilityMode::Sync;
  cfg.sync_batch_max_records = 5;
  cfg.sync_batch_interval_ms = 2000.0;
  std::uint64_t fsyncs = 0;
  {
    DurableLogWriter w(path, cfg);
    std::atomic<int> go{0};
    auto job = [&](int id) {
      while (go.load() == 0) {
        std::this_thread::yield();
      }
      for (int j = 0; j < 5; ++j) {
        std::vector<std::uint8_t> p = {'k', static_cast<std::uint8_t>(id), static_cast<std::uint8_t>(j)};
        w.append(p);
      }
    };
    std::thread t0(job, 0);
    std::thread t1(job, 1);
    go = 1;
    t0.join();
    t1.join();
    fsyncs = w.fsync_count();
  }
  if (fsyncs >= 10u) {
    throw std::runtime_error("group commit should use fewer than 10 fsyncs");
  }
  if (fsyncs < 2u) {
    throw std::runtime_error("expected at least 2 fsync batches");
  }
  ::unlink(path.c_str());
}

void test_fsync_hook() {
  const std::string path = make_tmp_wal();
  DurabilityManager::Config cfg;
  cfg.mode = DurabilityMode::Sync;
  cfg.sync_batch_max_records = 2;
  cfg.sync_batch_interval_ms = 50.0;
  int calls = 0;
  cfg.fsync_hook = [&calls](int fd) {
    ++calls;
    if (::fsync(fd) != 0) {
      throw std::runtime_error("hook fsync");
    }
  };
  {
    DurableLogWriter w(path, cfg);
    w.append(std::vector<std::uint8_t>{1});
    w.append(std::vector<std::uint8_t>{2});
  }
  if (calls < 1) {
    throw std::runtime_error("fsync_hook not invoked");
  }
  ::unlink(path.c_str());
}

void test_async_ordering_after_recover() {
  const std::string path = make_tmp_wal();
  DurabilityManager::Config cfg;
  cfg.mode = DurabilityMode::Async;
  cfg.async_flush_max_records = 4;
  cfg.async_flush_interval_ms = 20.0;
  {
    DurableLogWriter w(path, cfg);
    for (int i = 0; i < 12; ++i) {
      w.append(std::vector<std::uint8_t>{static_cast<std::uint8_t>(i)});
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
  }
  {
    DurableLogWriter w2(path, cfg);
    (void)w2;
  }
  LogReader r(path);
  std::vector<std::uint8_t> pl;
  std::uint64_t i = 0;
  while (r.read_next(pl)) {
    if (pl.size() != 1u || pl[0] != static_cast<std::uint8_t>(i)) {
      throw std::runtime_error("async ordering");
    }
    ++i;
  }
  ::unlink(path.c_str());
}

void test_fork_sync_child_survives() {
  const std::string path = make_tmp_wal();
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork");
  }
  if (pid == 0) {
    try {
      DurabilityManager::Config cfg;
      cfg.mode = DurabilityMode::Sync;
      cfg.sync_batch_max_records = 1;
      cfg.sync_batch_interval_ms = 99999.0;
      DurableLogWriter w(path, cfg);
      w.append(std::vector<std::uint8_t>{'x'});
      _exit(0);
    } catch (...) {
      _exit(2);
    }
  }
  int st = 0;
  if (::waitpid(pid, &st, 0) < 0) {
    ::unlink(path.c_str());
    throw std::runtime_error("waitpid");
  }
  if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) {
    ::unlink(path.c_str());
    throw std::runtime_error("child failed");
  }
  LogReader r(path);
  std::vector<std::uint8_t> pl;
  if (!r.read_next(pl) || pl != std::vector<std::uint8_t>{'x'}) {
    ::unlink(path.c_str());
    throw std::runtime_error("fork sync readback");
  }
  ::unlink(path.c_str());
}

}  // namespace

int main() {
  try {
    test_sync_readback();
    test_group_commit_fewer_fsync_than_appends();
    test_fsync_hook();
    test_async_ordering_after_recover();
    test_fork_sync_child_survives();
    std::cout << "durability_test: all ok\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
