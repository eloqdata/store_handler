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
DEFINE_string(eloq_store_cloud_store_path,
              "",
              "EloqStore cloud store path (disable cloud store if empty)");
DEFINE_uint32(
    eloq_store_gc_threads,
    1,
    "EloqStore gc threads count (Must be 0 when cloud store is enabled).");
DEFINE_uint32(eloq_store_cloud_worker_count,
              1,
              "EloqStore cloud worker count.");
DEFINE_uint32(eloq_store_data_page_restart_interval,
              16,
              "EloqStore data page restart interval.");
DEFINE_uint32(eloq_store_index_page_restart_interval,
              16,
              "EloqStore index page restart interval.");
DEFINE_uint32(eloq_store_init_page_count,
              1 << 15,
              "EloqStore initial page count.");
DEFINE_bool(eloq_store_skip_verify_checksum,
            false,
            "EloqStore skip verify checksum.");
DEFINE_uint32(eloq_store_index_buffer_pool_size,
              1 << 15,
              "EloqStore index buffer pool size.");
DEFINE_uint32(eloq_store_manifest_limit, 8 << 20, "EloqStore manifest limit.");
DEFINE_uint32(eloq_store_io_queue_size,
              4096,
              "EloqStore io queue size per shard.");
DEFINE_uint32(eloq_store_max_inflight_write,
              64 << 10,
              "EloqStore max inflight write.");
DEFINE_uint32(eloq_store_max_write_batch_pages,
              256,
              "EloqStore max write batch pages.");
DEFINE_uint32(eloq_store_buf_ring_size, 1 << 12, "EloqStore buf ring size.");
DEFINE_uint32(eloq_store_coroutine_stack_size,
              32 * 1024,
              "EloqStore coroutine stack size.");
DEFINE_uint32(eloq_store_num_retained_archives,
              0,
              "EloqStore num retained archives.");
DEFINE_uint32(eloq_store_archive_interval_secs,
              86400,
              "EloqStore archive interval secs.");
DEFINE_uint32(eloq_store_max_archive_tasks,
              256,
              "EloqStore max archive tasks.");
DEFINE_uint32(eloq_store_file_amplify_factor,
              4,
              "EloqStore file amplify factor.");
DEFINE_uint64(eloq_store_local_space_limit,
              1ULL << 40,
              "EloqStore local space limit.");
DEFINE_uint32(eloq_store_reserve_space_ratio,
              100,
              "EloqStore reserve space ratio.");
DEFINE_uint32(eloq_store_data_page_size, 1 << 12, "EloqStore data page size.");
DEFINE_uint32(eloq_store_pages_per_file_shift,
              11,
              "EloqStore pages per file shift.");
DEFINE_uint32(eloq_store_overflow_pointers, 16, "EloqStore overflow pointers.");
DEFINE_bool(eloq_store_data_append_mode, false, "EloqStore data append mode.");
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
    eloq_store_config.worker_count_ =
        std::max(eloq_store_config.worker_count_, uint16_t(1));
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

    eloq_store_config.cloud_store_path_ =
        !CheckCommandLineFlagIsDefault("eloq_store_cloud_store_path")
            ? FLAGS_eloq_store_cloud_store_path
            : config_reader.GetString("store",
                                      "eloq_store_cloud_store_path",
                                      FLAGS_eloq_store_cloud_store_path);
    eloq_store_config.gc_threads_ =
        !eloq_store_config.cloud_store_path_.empty()
            ? 0
            : (!CheckCommandLineFlagIsDefault("eloq_store_gc_threads")
                   ? FLAGS_eloq_store_gc_threads
                   : config_reader.GetInteger("store",
                                              "eloq_store_gc_threads",
                                              FLAGS_eloq_store_gc_threads));
    eloq_store_config.cloud_worker_count_ =
        !CheckCommandLineFlagIsDefault("eloq_store_cloud_worker_count")
            ? FLAGS_eloq_store_cloud_worker_count
            : config_reader.GetInteger("store",
                                       "eloq_store_cloud_worker_count",
                                       FLAGS_eloq_store_cloud_worker_count);
    LOG_IF(INFO, !eloq_store_config.cloud_store_path_.empty())
        << "EloqStore cloud store enabled";
    eloq_store_config.data_page_restart_interval_ =
        !CheckCommandLineFlagIsDefault("eloq_store_data_page_restart_interval")
            ? FLAGS_eloq_store_data_page_restart_interval
            : config_reader.GetInteger(
                  "store",
                  "eloq_store_data_page_restart_interval",
                  FLAGS_eloq_store_data_page_restart_interval);
    eloq_store_config.index_page_restart_interval_ =
        !CheckCommandLineFlagIsDefault("eloq_store_index_page_restart_interval")
            ? FLAGS_eloq_store_index_page_restart_interval
            : config_reader.GetInteger(
                  "store",
                  "eloq_store_index_page_restart_interval",
                  FLAGS_eloq_store_index_page_restart_interval);
    eloq_store_config.init_page_count_ =
        !CheckCommandLineFlagIsDefault("eloq_store_init_page_count")
            ? FLAGS_eloq_store_init_page_count
            : config_reader.GetInteger("store",
                                       "eloq_store_init_page_count",
                                       FLAGS_eloq_store_init_page_count);
    eloq_store_config.skip_verify_checksum_ =
        !CheckCommandLineFlagIsDefault("eloq_store_skip_verify_checksum")
            ? FLAGS_eloq_store_skip_verify_checksum
            : config_reader.GetBoolean("store",
                                       "eloq_store_skip_verify_checksum",
                                       FLAGS_eloq_store_skip_verify_checksum);
    eloq_store_config.index_buffer_pool_size_ =
        !CheckCommandLineFlagIsDefault("eloq_store_index_buffer_pool_size")
            ? FLAGS_eloq_store_index_buffer_pool_size
            : config_reader.GetInteger("store",
                                       "eloq_store_index_buffer_pool_size",
                                       FLAGS_eloq_store_index_buffer_pool_size);
    eloq_store_config.index_buffer_pool_size_ /=
        eloq_store_config.worker_count_;
    eloq_store_config.manifest_limit_ =
        !CheckCommandLineFlagIsDefault("eloq_store_manifest_limit")
            ? FLAGS_eloq_store_manifest_limit
            : config_reader.GetInteger("store",
                                       "eloq_store_manifest_limit",
                                       FLAGS_eloq_store_manifest_limit);
    eloq_store_config.io_queue_size_ =
        !CheckCommandLineFlagIsDefault("eloq_store_io_queue_size")
            ? FLAGS_eloq_store_io_queue_size
            : config_reader.GetInteger("store",
                                       "eloq_store_io_queue_size",
                                       FLAGS_eloq_store_io_queue_size);
    eloq_store_config.io_queue_size_ /= eloq_store_config.worker_count_;
    eloq_store_config.max_inflight_write_ =
        !CheckCommandLineFlagIsDefault("eloq_store_max_inflight_write")
            ? FLAGS_eloq_store_max_inflight_write
            : config_reader.GetInteger("store",
                                       "eloq_store_max_inflight_write",
                                       FLAGS_eloq_store_max_inflight_write);
    eloq_store_config.max_inflight_write_ /= eloq_store_config.worker_count_;
    eloq_store_config.max_write_batch_pages_ =
        !CheckCommandLineFlagIsDefault("eloq_store_max_write_batch_pages")
            ? FLAGS_eloq_store_max_write_batch_pages
            : config_reader.GetInteger("store",
                                       "eloq_store_max_write_batch_pages",
                                       FLAGS_eloq_store_max_write_batch_pages);
    eloq_store_config.buf_ring_size_ =
        !CheckCommandLineFlagIsDefault("eloq_store_buf_ring_size")
            ? FLAGS_eloq_store_buf_ring_size
            : config_reader.GetInteger("store",
                                       "eloq_store_buf_ring_size",
                                       FLAGS_eloq_store_buf_ring_size);
    eloq_store_config.coroutine_stack_size_ =
        !CheckCommandLineFlagIsDefault("eloq_store_coroutine_stack_size")
            ? FLAGS_eloq_store_coroutine_stack_size
            : config_reader.GetInteger("store",
                                       "eloq_store_coroutine_stack_size",
                                       FLAGS_eloq_store_coroutine_stack_size);
    eloq_store_config.num_retained_archives_ =
        !CheckCommandLineFlagIsDefault("eloq_store_num_retained_archives")
            ? FLAGS_eloq_store_num_retained_archives
            : config_reader.GetInteger("store",
                                       "eloq_store_num_retained_archives",
                                       FLAGS_eloq_store_num_retained_archives);
    eloq_store_config.archive_interval_secs_ =
        !CheckCommandLineFlagIsDefault("eloq_store_archive_interval_secs")
            ? FLAGS_eloq_store_archive_interval_secs
            : config_reader.GetInteger("store",
                                       "eloq_store_archive_interval_secs",
                                       FLAGS_eloq_store_archive_interval_secs);
    eloq_store_config.max_archive_tasks_ =
        !CheckCommandLineFlagIsDefault("eloq_store_max_archive_tasks")
            ? FLAGS_eloq_store_max_archive_tasks
            : config_reader.GetInteger("store",
                                       "eloq_store_max_archive_tasks",
                                       FLAGS_eloq_store_max_archive_tasks);
    eloq_store_config.file_amplify_factor_ =
        !CheckCommandLineFlagIsDefault("eloq_store_file_amplify_factor")
            ? FLAGS_eloq_store_file_amplify_factor
            : config_reader.GetInteger("store",
                                       "eloq_store_file_amplify_factor",
                                       FLAGS_eloq_store_file_amplify_factor);
    eloq_store_config.local_space_limit_ =
        !CheckCommandLineFlagIsDefault("eloq_store_local_space_limit")
            ? FLAGS_eloq_store_local_space_limit
            : config_reader.GetInteger("store",
                                       "eloq_store_local_space_limit",
                                       FLAGS_eloq_store_local_space_limit);
    eloq_store_config.local_space_limit_ /= eloq_store_config.worker_count_;
    eloq_store_config.reserve_space_ratio_ =
        !CheckCommandLineFlagIsDefault("eloq_store_reserve_space_ratio")
            ? FLAGS_eloq_store_reserve_space_ratio
            : config_reader.GetInteger("store",
                                       "eloq_store_reserve_space_ratio",
                                       FLAGS_eloq_store_reserve_space_ratio);
    eloq_store_config.data_page_size_ =
        !CheckCommandLineFlagIsDefault("eloq_store_data_page_size")
            ? FLAGS_eloq_store_data_page_size
            : config_reader.GetInteger("store",
                                       "eloq_store_data_page_size",
                                       FLAGS_eloq_store_data_page_size);
    eloq_store_config.pages_per_file_shift_ =
        !CheckCommandLineFlagIsDefault("eloq_store_pages_per_file_shift")
            ? FLAGS_eloq_store_pages_per_file_shift
            : config_reader.GetInteger("store",
                                       "eloq_store_pages_per_file_shift",
                                       FLAGS_eloq_store_pages_per_file_shift);
    eloq_store_config.overflow_pointers_ =
        !CheckCommandLineFlagIsDefault("eloq_store_overflow_pointers")
            ? FLAGS_eloq_store_overflow_pointers
            : config_reader.GetInteger("store",
                                       "eloq_store_overflow_pointers",
                                       FLAGS_eloq_store_overflow_pointers);
    eloq_store_config.data_append_mode_ =
        !CheckCommandLineFlagIsDefault("eloq_store_data_append_mode")
            ? FLAGS_eloq_store_data_append_mode
            : config_reader.GetBoolean("store",
                                       "eloq_store_data_append_mode",
                                       FLAGS_eloq_store_data_append_mode);
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
        if (!eloq_store_config.cloud_store_path_.empty())
        {
            store_config.cloud_store_path
                .append(eloq_store_config.cloud_store_path_)
                .append("/ds_")
                .append(std::to_string(shard_id));
        }
        store_config.num_gc_threads = eloq_store_config.gc_threads_;
        store_config.rclone_threads = eloq_store_config.cloud_worker_count_;
        store_config.data_page_restart_interval =
            eloq_store_config.data_page_restart_interval_;
        store_config.index_page_restart_interval =
            eloq_store_config.index_page_restart_interval_;
        store_config.init_page_count = eloq_store_config.init_page_count_;
        store_config.skip_verify_checksum =
            eloq_store_config.skip_verify_checksum_;
        store_config.index_buffer_pool_size =
            eloq_store_config.index_buffer_pool_size_;
        store_config.manifest_limit = eloq_store_config.manifest_limit_;
        store_config.io_queue_size = eloq_store_config.io_queue_size_;
        store_config.max_inflight_write = eloq_store_config.max_inflight_write_;
        store_config.max_write_batch_pages =
            eloq_store_config.max_write_batch_pages_;
        store_config.buf_ring_size = eloq_store_config.buf_ring_size_;
        store_config.coroutine_stack_size =
            eloq_store_config.coroutine_stack_size_;
        store_config.num_retained_archives =
            eloq_store_config.num_retained_archives_;
        store_config.archive_interval_secs =
            eloq_store_config.archive_interval_secs_;
        store_config.max_archive_tasks = eloq_store_config.max_archive_tasks_;
        store_config.file_amplify_factor =
            eloq_store_config.file_amplify_factor_;
        store_config.local_space_limit = eloq_store_config.local_space_limit_;
        store_config.reserve_space_ratio =
            eloq_store_config.reserve_space_ratio_;
        store_config.data_page_size = eloq_store_config.data_page_size_;
        store_config.pages_per_file_shift =
            eloq_store_config.pages_per_file_shift_;
        store_config.overflow_pointers = eloq_store_config.overflow_pointers_;
        store_config.data_append_mode = eloq_store_config.data_append_mode_;
        if (eloq_store_config.comparator_ != nullptr)
        {
            store_config.comparator_ = eloq_store_config.comparator_;
        }

        DLOG(INFO) << "Create EloqStore storage with workers: "
                   << store_config.num_threads
                   << ", store path: " << store_config.store_path.front()
                   << ", open files limit: " << store_config.fd_limit
                   << ", cloud store path: " << store_config.cloud_store_path
                   << ", gc threads: " << store_config.num_gc_threads
                   << ", cloud worker count: " << store_config.rclone_threads
                   << ", buffer pool size per shard: "
                   << store_config.index_buffer_pool_size;
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
