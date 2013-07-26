#include <cassert>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>

#include <boost/program_options.hpp>
#include <hiredis/hiredis.h>
#include <sys/wait.h>
#include <unistd.h>

namespace po = boost::program_options;

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

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help,h", "produce help message")
      ("host,H", po::value<std::string>()->default_value("127.0.0.1"),
       "host to connect to")
      ("port,p", po::value<std::uint16_t>()->default_value(6379),
       "port to connect on")
      ("concurrency,c", po::value<std::size_t>()->default_value(8),
       "concurrency level (number of workers)")
      ("num-writes,n", po::value<std::size_t>()->default_value(15000),
       "number of writes to issue per worker")
      ("key-size", po::value<std::size_t>()->default_value(20),
       "the key size, in bytes")
      ("value-size", po::value<std::size_t>()->default_value(500),
       "the value size, in bytes")
      ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc;
    return 0;
  }

  const std::string redis_host = vm["host"].as<std::string>();
  const std::uint16_t redis_port = vm["port"].as<std::uint16_t>();
  const std::size_t concurrency = vm["concurrency"].as<std::size_t>();

  // create a test context before proceeding with the forking
  // rigamarole, to ensure that we can actually connect to redis
  {
    RedisContext test_context(redis_host, redis_port);
    test_context.EnsureOk();
  }

  // create the worker children
  for (std::size_t i = 0; i < concurrency; i++) {
    int pid = fork();
    if (pid == -1) {
      perror("fork()");
      return 1;
    } else if (!pid) {
      run_process(redis_host,
                  redis_port,
                  vm["num-writes"].as<std::size_t>(),
                  vm["key-size"].as<std::size_t>(),
                  vm["value-size"].as<std::size_t>());
      return 0;
    }
  }

  // wait for the workers to finish
  for (std::size_t i = 0; i < concurrency; i++) {
    int status;
    pid_t pid = wait(&status);
    if (pid == -1) {
      perror("wait()");
      return 1;
    }
  }

  return 0;
}
