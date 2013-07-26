#include <cassert>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <sys/wait.h>
#include <unistd.h>

using boost::asio::ip::tcp;

namespace po = boost::program_options;

class XString {
 public:
  XString() = delete;
  XString(const char *data, std::size_t data_size)
      :data_(data), size_(data_size) {}

  inline const char *data() const { return data_; }
  inline std::size_t size() const { return size_; }

 private:
  const char *data_;
  std::size_t size_;
};

class RedisClient {
 public:
  RedisClient() = delete;
  RedisClient(const RedisClient &other) = delete;
  void operator=(const RedisClient &other) = delete;

  explicit RedisClient(boost::asio::io_service &io_service)
      :io_service_(io_service), socket_(io_service) {}

  void Connect(const std::string &host, std::uint16_t port) {
    const std::string str_port = boost::lexical_cast<std::string>(port);
    boost::asio::ip::tcp::resolver resolver(io_service_);
    boost::asio::ip::tcp::resolver::query query(
        host, str_port.c_str(),
        boost::asio::ip::resolver_query_base::numeric_service);
    boost::asio::ip::tcp::resolver::iterator iterator = resolver.resolve(query);
    boost::asio::connect(socket_, iterator);
  }

  bool Set(const XString &key, const XString &val) {
    std::ostream os(&buf_);
    os << "*3\r\n$3\r\nSET\r\n$" << key.size() << "\r\n";
    os.write(key.data(), key.size());
    os << "\r\n$" << val.size() << "\r\n";
    os.write(val.data(), val.size());
    os << "\r\n";

    std::size_t n = socket_.send(buf_.data());
    buf_.consume(n);
    return ExpectResponse();
  }

  bool FlushAll() {
    std::ostream os(&buf_);
    os << "*1\r\n$8\r\nFLUSHALL\r\n";
    std::size_t n = socket_.send(buf_.data());
    buf_.consume(n);
    return ExpectResponse();
  }

 private:
  boost::asio::io_service &io_service_;
  tcp::socket socket_;
  boost::asio::streambuf buf_;

  bool ExpectResponse() {
    boost::system::error_code error;
    boost::asio::read_until(socket_, buf_, "\r\n", error);
    std::istream response_str(&buf_);
    char status_char;
    response_str.get(status_char);
    if (status_char == '+') {
      buf_.consume(buf_.size());
      return true;
    } else {
      std::cerr << status_char;
      std::istream str(&buf_);
      std::string s;
      std::getline(str, s);
      std::cerr << s << std::endl;
      return false;
    }

  }
};

// Simple RAII interface around an array of C data that doesn't overallocate.
template <typename T>
class CompactVector {
 public:
  CompactVector() = delete;
  CompactVector(const CompactVector &other) = delete;
  void operator=(const CompactVector &other) = delete;

  CompactVector(std::size_t size)
      :size_(size), data_(new T[size]) {}

  inline T& operator[] (std::size_t index) { return data_.get()[index]; }

  inline std::size_t size() const { return size_; }

  inline T* data() { return data_.get(); }

 private:
  const std::size_t size_;
  std::unique_ptr<T[]> data_;
};

void run_process(const std::string &host,
                 const std::uint16_t port,
                 const std::size_t num_writes,
                 const std::size_t key_size,
                 const std::size_t val_size) {
  boost::asio::io_service io_service;
  RedisClient client(io_service);
  client.Connect(host, port);

  std::ifstream urandom("/dev/urandom");
  std::unique_ptr<char []> key(new char[key_size]);
  std::unique_ptr<char []> val(new char[val_size]);

  // Allocate a vector so we can get percentile values.... this should
  // be fairly reasonable, for a million keys, using longs, on a
  // 64-bit platform this uses 8MB of memory, which is not very
  // much at all, even if we have multiple workers.
  CompactVector<long> timings(num_writes);

  for (std::size_t i = 0; i < num_writes; i++) {
    urandom.read(key.get(), key_size);
    urandom.read(val.get(), val_size);

    std::chrono::time_point<std::chrono::system_clock> start = \
        std::chrono::system_clock::now();
    bool ok = client.Set(XString(key.get(), key_size),
                         XString(val.get(), val_size));
    long elapsed_micros = std::chrono::duration_cast<std::chrono::microseconds>
        (std::chrono::system_clock::now() - start).count();
    if (!ok) {
      std::cerr << "failed to set value!\n";
      exit(EXIT_FAILURE);
    }
    timings[i] = elapsed_micros;
  }

  // analyze the results
  std::sort(timings.data(), timings.data() + timings.size());
  long median = timings[num_writes / 2];
  long p95 = timings[num_writes * 19 / 20];
  long p99 = timings[num_writes * 99 / 100];
  long max = timings[num_writes - 1];

  // try to print the result atomically, using cout.write() instead of
  // the stream operators, since multiple processes may exit at once
  std::stringstream ss;
  ss << "pid " << getpid() << ", "
     << median << " " << p95 << " " << p99 << " " << max << "\n";
  std::string outline = ss.str();
  std::cout.write(outline.data(), outline.size());
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
      ("num-writes,n", po::value<std::size_t>()->default_value(100000),
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
    boost::asio::io_service io_service;
    RedisClient client(io_service);
    client.Connect(redis_host, redis_port);
    std::cout << "flushing all data..." << std::flush;
    bool status = client.FlushAll();
    if (!status) {
      std::cerr << "error flushing!\n";
      return 1;
    }
    std::cout << " done!" << std::endl;
  }

  std::cout << "format is: pid, median us, 95th us, 99th us, max us\n";

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
