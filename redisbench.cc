#include <cassert>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>

#include <hiredis/hiredis.h>
#include <sys/wait.h>
#include <unistd.h>

#define CONCURRENCY 8

class RedisContext {
 public:
  RedisContext() = delete;
  RedisContext(const RedisContext &other) = delete;
  void operator=(const RedisContext &other) = delete;

  RedisContext(const std::string &host, std::uint16_t port)
      :ctx_(redisConnect(host.c_str(), port)) {}

  // this can exit, naughty
  void EnsureOk() {
    if (ctx_ == nullptr) {
      std::cerr << "failed to allocate redisContext\n";
      exit(EXIT_FAILURE);
    }
    if (ctx_->err) {
      std::cerr << "connection error: " << ctx_->errstr << "\n";
      exit(EXIT_FAILURE);
    }
  }

  inline redisContext* ctx() { return ctx_; }

  ~RedisContext() { redisFree(ctx_); }

 private:
  redisContext *ctx_;
};

class Command {
 public:
  Command() = delete;
  Command(const Command &other) = delete;
  void operator=(const Command &other) = delete;

  Command(RedisContext *ctx, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    reply_ = reinterpret_cast<redisReply *>(redisvCommand(ctx->ctx(), fmt, ap));
    va_end(ap);
  }

  inline bool ok() const { return reply_ != nullptr; }

  inline redisReply* reply() { return reply_; }

  ~Command() { freeReplyObject(reply_); }

 private:
  redisReply *reply_;
};

void run_process(const std::string &host,
                 const std::uint16_t port,
                 const std::size_t num_writes,
                 const std::size_t key_size,
                 const std::size_t val_size) {
  RedisContext ctx(host, port);
  ctx.EnsureOk();

  std::ifstream urandom("/dev/urandom");
  std::unique_ptr<char []> key(new char[key_size]);
  std::unique_ptr<char []> val(new char[val_size]);

  long max_millis = 0;
  for (std::size_t i = 0; i < num_writes; i++) {
    urandom.read(key.get(), key_size);
    urandom.read(val.get(), val_size);

    std::chrono::time_point<std::chrono::system_clock> start = \
        std::chrono::system_clock::now();
    Command cmd(&ctx, "SET %b %b", key.get(), key_size, val.get(), val_size);
    long elapsed_millis = std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now() - start).count();
    if (!cmd.ok()) {
      std::cerr << "failed to set value!\n";
      exit(EXIT_FAILURE);
    }
    max_millis = std::max(max_millis, elapsed_millis);
  }

  // try to print the result atomically, since multiple processes may
  // exit at once
  std::stringstream ss;
  ss << "pid " << getpid() << ", max millis: " << max_millis;
  puts(ss.str().c_str());
}

int main() {
  // create a test context before proceeding with the forking
  // rigamarole, to ensure that we can actually connect to redis
  {
    RedisContext test_context("127.0.0.1", 6379);
    test_context.EnsureOk();
  }

  // create the worker children
  for (int i = 0; i < CONCURRENCY; i++) {
    int pid = fork();
    if (pid == -1) {
      perror("fork()");
      return 1;
    } else if (!pid) {
      run_process("127.0.0.1", 6379, 20, 500, 100000);
      return 0;
    }
  }

  // wait for them to finish
  for (int i = 0; i < CONCURRENCY; i++) {
    int status;
    pid_t pid = wait(&status);
    if (pid == -1) {
      perror("wait()");
      return 1;
    }
  }

  return 0;
}
