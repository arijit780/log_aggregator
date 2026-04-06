#include "log_storage/io_util.hpp"
#include "log_storage/log_reader.hpp"
#include "log_storage/log_writer.hpp"

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

std::string temp_log_path() {
  std::string p = "/tmp/log_crash_XXXXXX";
  std::vector<char> buf(p.begin(), p.end());
  buf.push_back('\0');
  const int fd = ::mkstemp(buf.data());
  if (fd < 0) {
    throw std::runtime_error("mkstemp failed");
  }
  ::close(fd);
  return std::string(buf.data());
}

void make_payload(std::uint64_t offset, std::vector<std::uint8_t>& out) {
  out.resize(8 + static_cast<std::size_t>(offset % 57));
  log_storage::pack_le_u64(out.data(), offset);
  for (std::size_t i = 8; i < out.size(); ++i) {
    out[i] = static_cast<std::uint8_t>((offset + static_cast<std::uint64_t>(i)) & 0xFFu);
  }
}

bool payload_matches_offset(const std::vector<std::uint8_t>& p, std::uint64_t off) {
  if (p.size() < 8) {
    return false;
  }
  std::uint64_t v = 0;
  log_storage::unpack_le_u64(p.data(), &v);
  if (v != off) {
    return false;
  }
  for (std::size_t i = 8; i < p.size(); ++i) {
    if (p[i] != static_cast<std::uint8_t>((off + static_cast<std::uint64_t>(i)) & 0xFFu)) {
      return false;
    }
  }
  return true;
}

std::uint64_t read_all_valid_records(const std::string& path) {
  log_storage::LogReader reader(path);
  std::vector<std::uint8_t> pl;
  std::uint64_t idx = 0;
  while (reader.read_next(pl)) {
    if (!payload_matches_offset(pl, idx)) {
      throw std::runtime_error("payload mismatch for sequential offset");
    }
    ++idx;
  }
  if (reader.expected_next_offset() != idx) {
    throw std::runtime_error("reader next offset inconsistent");
  }
  return idx;
}

void append_until_killed(const std::string& path) {
  log_storage::LogWriter w(path);
  std::vector<std::uint8_t> payload;
  for (std::uint64_t i = 0;; ++i) {
    make_payload(i, payload);
    w.append(payload);
    if ((i & 0x3FFu) == 0u) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  }
}

void test_corrupt_tail_truncates() {
  const std::string path = temp_log_path();
  {
    log_storage::LogWriter w(path);
    std::vector<std::uint8_t> payload;
    for (std::uint64_t i = 0; i < 20; ++i) {
      make_payload(i, payload);
      w.append(payload);
    }
  }
  {
    const int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
      throw std::runtime_error("open for corrupt");
    }
    const off_t sz = ::lseek(fd, 0, SEEK_END);
    if (sz < 4) {
      ::close(fd);
      throw std::runtime_error("unexpected small log");
    }
    if (::ftruncate(fd, sz - 3) < 0) {
      ::close(fd);
      throw std::runtime_error("ftruncate corrupt");
    }
    ::close(fd);
  }
  std::uint64_t n = 0;
  {
    log_storage::LogWriter w(path);  // recovery truncates torn suffix
    n = w.next_offset();
  }
  const std::uint64_t read_back = read_all_valid_records(path);
  if (read_back != n) {
    throw std::runtime_error("reader count vs recovered next_offset mismatch");
  }
  if (n == 20) {
    throw std::runtime_error("expected partial last record to be dropped");
  }
  ::unlink(path.c_str());
}

}  // namespace

int main() {
  std::mt19937_64 rng(std::random_device{}());

  test_corrupt_tail_truncates();
  std::cout << "corrupt_tail_truncates ok\n";

  constexpr int kTrials = 40;
  int passed = 0;

  for (int t = 0; t < kTrials; ++t) {
    const std::string path = temp_log_path();

    const pid_t pid = ::fork();
    if (pid < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid == 0) {
      try {
        append_until_killed(path);
      } catch (...) {
        _exit(2);
      }
      _exit(0);
    }

    std::uniform_int_distribution<int> delay_us(200, 12000);
    std::this_thread::sleep_for(std::chrono::microseconds(delay_us(rng)));
    ::kill(pid, SIGKILL);
    int st = 0;
    (void)::waitpid(pid, &st, 0);

    try {
      log_storage::LogWriter reopened(path);
      const std::uint64_t count = read_all_valid_records(path);
      if (reopened.next_offset() != count) {
        throw std::runtime_error("count vs next_offset mismatch after crash");
      }
    } catch (const std::exception& e) {
      std::cerr << "trial " << t << " failed: " << e.what() << '\n';
      ::unlink(path.c_str());
      return 1;
    }

    ::unlink(path.c_str());
    ++passed;
  }

  std::cout << "crash_simulation: " << passed << "/" << kTrials << " trials ok\n";
  return 0;
}
