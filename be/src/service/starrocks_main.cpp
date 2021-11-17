// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/doris_main.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gperftools/malloc_extension.h>
#include <sys/file.h>
#include <unistd.h>

#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include <curl/curl.h>
#include <gperftools/profiler.h>
#include <thrift/TOutput.h>

#include "agent/heartbeat_server.h"
#include "agent/status.h"
#include "common/config.h"
#include "common/daemon.h"
#include "common/logging.h"
#include "common/resource_tls.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/heartbeat_flags.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/brpc_service.h"
#include "service/http_service.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/logging.h"
#include "util/network_util.h"
#include "util/starrocks_metrics.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

#include <dlfcn.h>

extern "C" {
void __lsan_do_leak_check();
}

using std::string;

namespace starrocks {
extern bool k_starrocks_exit;

starrocks::ThriftServer* be_server = nullptr;
std::unique_ptr<starrocks::BRpcService> brpc_service = nullptr;
std::unique_ptr<starrocks::HttpService> http_service = nullptr;
starrocks::ThriftServer* heartbeat_thrift_server = nullptr;

static void thrift_output(const char* x) {
    LOG(WARNING) << "thrift internal message: " << x;
}

} // namespace starrocks

// extern int meta_tool_main(int argc, char** argv);

static void usage(string progname) {
    std::cout << progname << " is the StarRocks backend server.\n\n";
    std::cout << "Usage:\n  " << progname << " [OPTION]...\n\n";
    std::cout << "Options:\n";
    std::cout << "  -v, --version      output version information, then exit\n";
    std::cout << "  -?, --help         show this help, then exit\n";
}

static bool write_pid() {
    auto pid_file = string(getenv("PID_DIR")) + "/be.pid";
    auto fd = open(pid_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd < 0) {
        std::cerr << "Failed to create pid file, err: " << strerror(errno) << std::endl;
        return false;
    }

    auto pid = std::to_string((long)getpid()) + "\n";
    auto len = write(fd, pid.c_str(), pid.size());
    if (len != pid.size()) {
        std::cerr << "Failed to write pid, err: " << strerror(errno) << std::endl;
    }

    if (close(fd) < 0) {
        std::cerr << "Failed to close pid file, err: " << strerror(errno) << std::endl;
        return false;
    }
    return true;
}

static bool start_servers(starrocks::ExecEnv *env) {
    starrocks::ThriftRpcHelper::setup(env);
    auto status = starrocks::BackendService::create_service(env, starrocks::config::be_port, &starrocks::be_server);
    if (!status.ok()) {
        return false;
    }

    status = starrocks::be_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start thrift server on port " << starrocks::config::be_port
                   << ", err: " << status.to_string();
        return false;
    }

    LOG(INFO) << "Started thrift server on port " << starrocks::config::be_port;

    starrocks::brpc_service = std::make_unique<starrocks::BRpcService>(env);
    status = starrocks::brpc_service->start(starrocks::config::brpc_port);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start brpc server on port " << starrocks::config::brpc_port
                   << ", err: " << status.to_string();
        return false;
    }

    LOG(INFO) << "Started rpc server on port " << starrocks::config::brpc_port;

    starrocks::http_service = std::make_unique<starrocks::HttpService>(
        env, starrocks::config::webserver_port,
        starrocks::config::webserver_num_workers);
    status = starrocks::http_service->start();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start http server on port " << starrocks::config::webserver_port
                   << ", err: " << status.to_string();
        return false;
    }

    LOG(INFO) << "Started http server on port " << starrocks::config::webserver_port;

    if (!create_and_start_heartbeat_server(env, env->master_info())) {
        return false;
    }

    LOG(INFO) << "Started heartbeat server on port " << starrocks::config::heartbeat_service_port;

    return true;
}

void stop_servers() {
    starrocks::heartbeat_thrift_server->stop();
    starrocks::heartbeat_thrift_server->join();
    delete starrocks::heartbeat_thrift_server;
    starrocks::heartbeat_thrift_server = nullptr;

    starrocks::http_service.reset();
    starrocks::brpc_service.reset();

    starrocks::be_server->stop();
    starrocks:: be_server->join();
    delete starrocks::be_server;
    starrocks::be_server = nullptr;
}

int main(int argc, char** argv) {
    // if (argc > 1 && strcmp(argv[1], "meta_tool") == 0) {
    //     return meta_tool_main(argc - 1, argv + 1);
    // }

    if (argc > 1) {
        if (strncmp(argv[1], "--version", 9) == 0 || strncmp(argv[1], "-v", 2) == 0) {
            std::cout << starrocks::get_build_version(false);
            return 0;
        } else if (strncmp(argv[1], "--help", 6) == 0 || strncmp(argv[1], "-?", 2) == 0) {
            usage(basename(argv[0]));
            return 0;
        }
    }

    if (getenv("STARROCKS_HOME") == nullptr) {
        std::cerr << "Please set STARROCKS_HOME environment variable" << std::endl;
        return -1;
    }

    // using starrocks::Status;
    if (!write_pid()) {
        return -1;
    }

    string config_file = string(getenv("STARROCKS_HOME")) + "/conf/be.conf";
    if (!starrocks::config::init(config_file.c_str(), true)) {
        std::cerr << "Failed to init config file.\n";
        return -1;
    }

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
    // Aggressive decommit is required so that unused pages in the TCMalloc page heap are
    // not backed by physical pages and do not contribute towards memory consumption.
    //
    // 2020-08-31: Disable aggressive decommit,  which will decrease the performance of
    // memory allocation and deallocation.
    // Change the total TCMalloc thread cache size if necessary.
    if (!MallocExtension::instance()->SetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                                         starrocks::config::tc_max_total_thread_cache_bytes)) {
        std::cout << "Failed to change TCMalloc total thread cache size.\n";
        return -1;
    }
#endif

    std::vector<starrocks::StorePath> paths;
    auto res = starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths);
    if (!res) {
        return -1;
    }

    // initilize libcurl here to avoid concurrent initialization
    auto curl_ret = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_ret != 0) {
        LOG(ERROR) << "Failed to init libcurl, ret: " << curl_ret;
        return -1;
    }

    // add logger for thrift internal
    apache::thrift::GlobalOutput.setOutputFunction(starrocks::thrift_output);

    starrocks::init_daemon(argc, argv, paths);

    starrocks::ResourceTls::init();

    if (!starrocks::BackendOptions::init()) {
        return -1;
    }

    auto env = starrocks::ExecEnv::GetInstance();
    auto status = starrocks::ExecEnv::init(env, paths);
    if (!status.ok()) {
        return -1;
    }

    auto engine = std::make_shared<starrocks::StorageEngine>(starrocks::EngineOptions::new_options(paths, env));
    if (!engine->init(env)) {
        return -1;
    }

    if (!start_servers(env)) {
        starrocks::shutdown_logging();
        return -1;
    }

    while (!starrocks::k_starrocks_exit) {
        sleep(10);
    }

    stop_servers();

    engine->stop();
    starrocks::ExecEnv::destroy(env);

    return 0;
}


