/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <filesystem>

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#else
#ifndef GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = google;
#endif
#endif

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
#include <aws/core/Aws.h>
#endif

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                       \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
#include "rocksdb_cloud_data_store.h"
#include "rocksdb_cloud_data_store_factory.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
#include "rocksdb_data_store.h"
#include "rocksdb_data_store_factory.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
#include "eloq_store_data_store_factory.h"
#endif

#include "data_store_service.h"

using namespace EloqDS;

DEFINE_string(config, "", "Configuration (*.ini)");

DEFINE_string(eloq_dss_peer_node,
              "",
              "Data store peer node address. Used to get cluster topology if "
              "data_store_config_file is not provided.");

DEFINE_string(ip, "127.0.0.1", "Server IP");
DEFINE_int32(port, 9100, "Server Port");

DEFINE_string(data_path, "./data", "Directory path to save data.");

DEFINE_string(log_file_name_prefix,
              "eloq_dss.log",
              "Sets the prefix for log files. Default is 'eloq_dss.log'");

DEFINE_bool(enable_cache_replacement, true, "Enable cache replacement");

DEFINE_bool(bootstrap,
            false,
            "Init data store config file and exit. (Only support bootstrap one "
            "node now.)");

#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
DEFINE_uint32(eloq_store_worker_num, 1, "EloqStore server worker num.");

DEFINE_string(eloq_store_data_path,
              "",
              "The data path of the EloqStore (default is "
              "'{eloq_data_path}/eloq_dss/eloqstore_data').");
DEFINE_uint32(eloq_store_open_files_limit,
              1024,
              "EloqStore maximum open files.");
#endif

static bool CheckCommandLineFlagIsDefault(const char *name)
{
    gflags::CommandLineFlagInfo flag_info;

    bool flag_found = gflags::GetCommandLineFlagInfo(name, &flag_info);
    // Make sure the flag is declared.
    assert(flag_found);
    (void) flag_found;

    // Return `true` if the flag has the default value and has not been set
    // explicitly from the cmdline or via SetCommandLineOption
    return flag_info.is_default;
}

void PrintHelloText()
{
    std::cout << "* Welcome to use DataStoreService Server." << std::endl;
    std::cout << "* Running logs will be written to the following path:"
              << std::endl;
    std::cout << FLAGS_log_dir << std::endl;
    std::cout << "* The above log path can be specified by arg --log_dir."
              << std::endl;
    std::cout << "* You can also run with [--help] for all available flags."
              << std::endl;
    std::cout << std::endl;
}

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
std::unique_ptr<Aws::SDKOptions> aws_options_;
#endif
std::unique_ptr<EloqDS::DataStoreService> data_store_service_;

void ShutDown()
{
    LOG(INFO) << "Stopping DataStoreService Server ...";

    if (data_store_service_ != nullptr)
    {
        data_store_service_->DisconnectDataStore();
        data_store_service_ = nullptr;
    }

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    Aws::ShutdownAPI(*aws_options_);
    aws_options_ = nullptr;
#endif

    if (!FLAGS_alsologtostderr)
    {
        std::cout << "DataStoreService Server Stopped." << std::endl;
    }
    LOG(INFO) << "DataStoreService Server Stopped.";

#if BRPC_WITH_GLOG
    google::ShutdownGoogleLogging();
#endif
}

int main(int argc, char *argv[])
{
    // Increase max allowed rpc message size to 512mb.
    GFLAGS_NAMESPACE::SetCommandLineOption("max_body_size", "536870912");
    GFLAGS_NAMESPACE::SetCommandLineOption("graceful_quit_on_sigterm", "true");
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_GLOG
    InitGoogleLogging(argv);
#endif

    FLAGS_stderrthreshold = google::GLOG_FATAL;
    if (!FLAGS_alsologtostderr)
    {
        PrintHelloText();
        std::cout << "Starting DataStoreService Server..." << std::endl;
    }

    INIReader config_reader(FLAGS_config);

    if (!FLAGS_config.empty() && config_reader.ParseError() != 0)
    {
        if (!FLAGS_alsologtostderr)
        {
            std::cout << "Failed to start, error: Can't load config file."
                      << std::endl;
        }
        LOG(ERROR) << "Failed to start, Error: Can't load config file.";

        ShutDown();
        return 0;
    }

    std::string eloq_dss_peer_node =
        !CheckCommandLineFlagIsDefault("eloq_dss_peer_node")
            ? FLAGS_eloq_dss_peer_node
            : config_reader.GetString(
                  "store", "eloq_dss_peer_node", FLAGS_eloq_dss_peer_node);

    std::string local_ip =
        !CheckCommandLineFlagIsDefault("ip")
            ? FLAGS_ip
            : config_reader.GetString("local", "ip", FLAGS_ip);

    uint16_t local_port =
        !CheckCommandLineFlagIsDefault("port")
            ? FLAGS_port
            : config_reader.GetInteger("local", "port", FLAGS_port);

    std::string data_path =
        !CheckCommandLineFlagIsDefault("data_path")
            ? FLAGS_data_path
            : config_reader.GetString("local", "data_path", FLAGS_data_path);

    if (!std::filesystem::exists(data_path))
    {
        std::filesystem::create_directories(data_path);
    }

    std::string ds_config_file_path = data_path + "/dss_config.ini";

    EloqDS::DataStoreServiceClusterManager ds_config;
    if (std::filesystem::exists(ds_config_file_path))
    {
        bool load_res = ds_config.Load(ds_config_file_path);
        if (!load_res)
        {
            LOG(ERROR) << "Failed to load config file: " << ds_config_file_path;
            ShutDown();
            return 0;
        }
    }
    else
    {
        if (FLAGS_bootstrap)
        {
            // Initialize the data store service config
            ds_config.Initialize(local_ip, local_port);
            if (!ds_config.Save(ds_config_file_path))
            {
                LOG(ERROR) << "Failed to save config to file: "
                           << ds_config_file_path;
                ShutDown();
                return 0;
            }
            LOG(INFO) << "bootstrap done !!!";
            ShutDown();
            return 0;
        }

        if (!eloq_dss_peer_node.empty())
        {
            ds_config.SetThisNode(local_ip, local_port);
            // Fetch ds topology from peer node
            if (!EloqDS::DataStoreService::FetchConfigFromPeer(
                    eloq_dss_peer_node, ds_config))
            {
                LOG(ERROR) << "Failed to fetch config from peer node: "
                           << eloq_dss_peer_node;
                ShutDown();
                return 0;
            }

            // Save the fetched config to the local file
            if (!ds_config.Save(ds_config_file_path))
            {
                LOG(ERROR) << "Failed to save config to file: "
                           << ds_config_file_path;
                ShutDown();
                return 0;
            }
        }
        else
        {
            // SingleNode: Initialize the data store service config and save.
            ds_config.Initialize(local_ip, local_port);
            if (!ds_config.Save(ds_config_file_path))
            {
                LOG(ERROR) << "Failed to save config to file: "
                           << ds_config_file_path;
                ShutDown();
                return 0;
            }
        }
    }

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    aws_options_ = std::make_unique<Aws::SDKOptions>();

    aws_options_->loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    Aws::InitAPI(*aws_options_);
#endif

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                       \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
    bool enable_cache_replacement_ = FLAGS_enable_cache_replacement;
    bool is_single_node = eloq_dss_peer_node.empty();

    // INIReader config_reader(nullptr, 0);
    EloqDS::RocksDBConfig rocksdb_config(config_reader, data_path);
    EloqDS::RocksDBCloudConfig rocksdb_cloud_config(config_reader);
    auto ds_factory = std::make_unique<EloqDS::RocksDBCloudDataStoreFactory>(
        rocksdb_config, rocksdb_cloud_config, enable_cache_replacement_);

#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
    bool enable_cache_replacement_ = FLAGS_enable_cache_replacement;
    bool is_single_node = eloq_dss_peer_node.empty();

    EloqDS::RocksDBConfig rocksdb_config(config_reader, data_path);
    auto ds_factory = std::make_unique<EloqDS::RocksDBDataStoreFactory>(
        rocksdb_config, enable_cache_replacement_);

#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    EloqDS::EloqStoreConfig eloq_store_config;
    eloq_store_config.worker_count_ =
        !CheckCommandLineFlagIsDefault("eloq_store_worker_num")
            ? FLAGS_eloq_store_worker_num
            : config_reader.GetInteger("store",
                                       "eloq_store_worker_num",
                                       FLAGS_eloq_store_worker_num);
    eloq_store_config.storage_path_ =
        !CheckCommandLineFlagIsDefault("eloq_store_data_path")
            ? FLAGS_eloq_store_data_path
            : config_reader.GetString(
                  "store", "eloq_store_data_path", FLAGS_eloq_store_data_path);
    if (eloq_store_config.storage_path_.empty())
    {
        eloq_store_config.storage_path_ = data_path + "/eloqstore_data";
        if (!std::filesystem::exists(eloq_store_config.storage_path_))
        {
            std::filesystem::create_directories(
                eloq_store_config.storage_path_);
        }
    }

    eloq_store_config.open_files_limit_ =
        !CheckCommandLineFlagIsDefault("eloq_store_open_files_limit")
            ? FLAGS_eloq_store_open_files_limit
            : config_reader.GetInteger("store",
                                       "eloq_store_open_files_limit",
                                       FLAGS_eloq_store_open_files_limit);
    auto ds_factory =
        std::make_unique<EloqDS::EloqStoreDataStoreFactory>(eloq_store_config);

#ifdef ELOQ_MODULE_ENABLED
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "bthread_concurrency",
        std::to_string(eloq_store_config.worker_count_).c_str());
#endif

#else
    assert(false);
    std::unique_ptr<DataStoreFactory> ds_factory = nullptr;
#endif

    data_store_service_ =
        std::make_unique<EloqDS::DataStoreService>(ds_config,
                                                   ds_config_file_path,
                                                   data_path + "/DSMigrateLog",
                                                   std::move(ds_factory));
    std::vector<uint32_t> dss_shards = ds_config.GetShardsForThisNode();
    std::unordered_map<uint32_t, std::unique_ptr<EloqDS::DataStore>>
        dss_shards_map;
    // setup rocksdb cloud data store
    for (int shard_id : dss_shards)
    {
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                       \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
        // TODO(lzx): move setup datastore to data_store_service
        auto ds = std::make_unique<EloqDS::RocksDBCloudDataStore>(
            rocksdb_cloud_config,
            rocksdb_config,
            (FLAGS_bootstrap || is_single_node),
            enable_cache_replacement_,
            shard_id,
            data_store_service_.get());
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
        auto ds = std::make_unique<EloqDS::RocksDBDataStore>(
            rocksdb_config,
            (FLAGS_bootstrap || is_single_node),
            enable_cache_replacement_,
            shard_id,
            data_store_service_.get());

#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
        ::eloqstore::KvOptions store_config;
        store_config.num_threads = eloq_store_config.worker_count_;
        store_config.store_path.emplace_back()
            .append(eloq_store_config.storage_path_)
            .append("/ds_")
            .append(std::to_string(shard_id));
        store_config.fd_limit = eloq_store_config.open_files_limit_;

        DLOG(INFO) << "Create EloqStore storage with workers: "
                   << store_config.num_threads
                   << ", store path: " << store_config.store_path.front()
                   << ", open files limit: " << store_config.fd_limit;
        auto ds = std::make_unique<EloqDS::EloqStoreDataStore>(
            shard_id, data_store_service_.get(), store_config);
#else
        assert(false);
        std::unique_ptr<DataStore> ds = nullptr;
#endif
        ds->Initialize();

        // Start db if the shard status is not closed
        if (ds_config.FetchDSShardStatus(shard_id) !=
            EloqDS::DSShardStatus::Closed)
        {
            bool ret = ds->StartDB();
            if (!ret)
            {
                LOG(ERROR)
                    << "Failed to start db instance in data store service";
                ShutDown();
                return 0;
            }
        }
        dss_shards_map[shard_id] = std::move(ds);
    }

    // setup local data store service
    bool ret = data_store_service_->StartService();
    if (!ret)
    {
        LOG(ERROR) << "Failed to start data store service";
        ShutDown();
        return 0;
    }
    data_store_service_->ConnectDataStore(std::move(dss_shards_map));

    if (!FLAGS_alsologtostderr)
    {
        std::cout << "DataStoreService Server Started, listening on "
                  << local_port << std::endl;
    }
    LOG(INFO) << "====DataStoreService Server Started, listening on "
              << local_port << "====";

    brpc::Server *server_ptr = data_store_service_->GetBrpcServer();
    server_ptr->RunUntilAskedToQuit();

    ShutDown();
    return 0;
}
