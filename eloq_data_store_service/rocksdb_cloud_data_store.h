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
#pragma once

#include <rocksdb/db.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "data_store.h"
#include "data_store_service.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb_config.h"
// #include "tx_service/include/cc/local_cc_shards.h"

namespace EloqDS
{

// Key separator for building key in RocksDB
static constexpr char KEY_SEPARATOR[] = "/";

// Use most significant bit (MSB) of version_ts to indicate if the ttl is set
constexpr uint64_t MSB = 1ULL << 63;  // Mask for Bit 63
constexpr uint64_t MSB_MASK = ~MSB;   // Mask to clear Bit 63

class TTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    bool Filter(int level,
                const rocksdb::Slice &key,
                const rocksdb::Slice &existing_value,
                std::string *new_value,
                bool *value_changed) const override
    {
        assert(existing_value.size() >= sizeof(uint64_t));
        bool has_ttl = false;
        uint64_t ts = *(reinterpret_cast<const uint64_t *>(
            existing_value.data() + sizeof(uint64_t)));

        // Check if the MSB is set
        if (ts & MSB)
        {
            has_ttl = true;
            ts &= MSB_MASK;  // Clear the MSB
        }
        else
        {
            has_ttl = false;
        }

        if (has_ttl)
        {
            assert(existing_value.size() >= sizeof(uint64_t) * 2);
            uint64_t rec_ttl = *(reinterpret_cast<const uint64_t *>(
                existing_value.data() + sizeof(uint64_t)));
            // Get the current timestamp in microseconds
            // auto current_timestamp =
            // txservice::LocalCcShards::ClockTsInMillseconds();
            // FIXME(lzx): only fetch the time at the begin of compaction.
            uint64_t current_timestamp =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now()
                        .time_since_epoch())
                    .count();

            // Check if the timestamp is smaller than the current timestamp
            if (rec_ttl < current_timestamp)
            {
                return true;  // Mark the key for deletion
            }
        }

        return false;  // Keep the key
    }

    const char *Name() const override
    {
        return "TTLCompactionFilter";
    }
};

// RocksDBEventListener is used to listen the flush event of RocksDB for
// recording unexpected write slow and stall when flushing
class RocksDBEventListener : public rocksdb::EventListener
{
public:
    void OnCompactionBegin(rocksdb::DB *db,
                           const rocksdb::CompactionJobInfo &ci) override
    {
        DLOG(INFO) << "Compaction begin, job_id: " << ci.job_id
                   << " ,thread: " << ci.thread_id
                   << " ,output_level: " << ci.output_level
                   << " ,input_files_size: " << ci.input_files.size()
                   << " ,compaction_reason: "
                   << static_cast<int>(ci.compaction_reason);
    }

    void OnCompactionCompleted(rocksdb::DB *db,
                               const rocksdb::CompactionJobInfo &ci) override
    {
        DLOG(INFO) << "Compaction end, job_id: " << ci.job_id
                   << " ,thread: " << ci.thread_id
                   << " ,output_level: " << ci.output_level
                   << " ,input_files_size: " << ci.input_files.size()
                   << " ,compaction_reason: "
                   << static_cast<int>(ci.compaction_reason);
    }

    void OnFlushBegin(rocksdb::DB *db,
                      const rocksdb::FlushJobInfo &flush_job_info) override
    {
        if (flush_job_info.triggered_writes_slowdown ||
            flush_job_info.triggered_writes_stop)
        {
            LOG(INFO) << "Flush begin, file: " << flush_job_info.file_path
                      << " ,job_id: " << flush_job_info.job_id
                      << " ,thread: " << flush_job_info.thread_id
                      << " ,file_number: " << flush_job_info.file_number
                      << " ,triggered_writes_slowdown: "
                      << flush_job_info.triggered_writes_slowdown
                      << " ,triggered_writes_stop: "
                      << flush_job_info.triggered_writes_stop
                      << " ,smallest_seqno: " << flush_job_info.smallest_seqno
                      << " ,largest_seqno: " << flush_job_info.largest_seqno
                      << " ,flush_reason: "
                      << GetFlushReason(flush_job_info.flush_reason);
        }
    }

    void OnFlushCompleted(rocksdb::DB *db,
                          const rocksdb::FlushJobInfo &flush_job_info) override
    {
        if (flush_job_info.triggered_writes_slowdown ||
            flush_job_info.triggered_writes_stop)
        {
            LOG(INFO) << "Flush end, file: " << flush_job_info.file_path
                      << " ,job_id: " << flush_job_info.job_id
                      << " ,thread: " << flush_job_info.thread_id
                      << " ,file_number: " << flush_job_info.file_number
                      << " ,triggered_writes_slowdown: "
                      << flush_job_info.triggered_writes_slowdown
                      << " ,triggered_writes_stop: "
                      << flush_job_info.triggered_writes_stop
                      << " ,smallest_seqno: " << flush_job_info.smallest_seqno
                      << " ,largest_seqno: " << flush_job_info.largest_seqno
                      << " ,flush_reason: "
                      << GetFlushReason(flush_job_info.flush_reason);
        }
    }

    std::string GetFlushReason(rocksdb::FlushReason flush_reason)
    {
        switch (flush_reason)
        {
        case rocksdb::FlushReason::kOthers:
            return "kOthers";
        case rocksdb::FlushReason::kGetLiveFiles:
            return "kGetLiveFiles";
        case rocksdb::FlushReason::kShutDown:
            return "kShutDown";
        case rocksdb::FlushReason::kExternalFileIngestion:
            return "kExternalFileIngestion";
        case rocksdb::FlushReason::kManualCompaction:
            return "kManualCompaction";
        case rocksdb::FlushReason::kWriteBufferManager:
            return "kWriteBufferManager";
        case rocksdb::FlushReason::kWriteBufferFull:
            return "kWriteBufferFull";
        case rocksdb::FlushReason::kTest:
            return "kTest";
        case rocksdb::FlushReason::kDeleteFiles:
            return "kDeleteFiles";
        case rocksdb::FlushReason::kAutoCompaction:
            return "kAutoCompaction";
        case rocksdb::FlushReason::kManualFlush:
            return "kManualFlush";
        case rocksdb::FlushReason::kErrorRecovery:
            return "kErrorRecovery";
        case rocksdb::FlushReason::kErrorRecoveryRetryFlush:
            return "kErrorRecoveryRetryFlush";
        case rocksdb::FlushReason::kWalFull:
            return "kWalFull";
        default:
            return "unknown";
        }
    }
};

/**
 * @brief Wrapper class for cache iterator with TTL at the data store service to
 * avoid creating iterator for each scan request from client.
 */
class RocksDBIteratorTTLWrapper : public TTLWrapper
{
public:
    explicit RocksDBIteratorTTLWrapper(rocksdb::Iterator *iter) : iter_(iter)
    {
    }

    ~RocksDBIteratorTTLWrapper()
    {
        delete iter_;
    }

    rocksdb::Iterator *GetIter()
    {
        return iter_;
    }

private:
    rocksdb::Iterator *iter_;
};

class RocksDBCloudDataStore : public DataStore
{
public:
    RocksDBCloudDataStore(const EloqDS::RocksDBCloudConfig &cloud_config,
                          const EloqDS::RocksDBConfig &config,
                          bool create_if_missing,
                          bool tx_enable_cache_replacement,
                          uint32_t shard_id,
                          DataStoreService *data_store_service);

    ~RocksDBCloudDataStore();

    /**
     * @brief Build S3 client factory.
     *        Used to connect to S3 compatible storage, e.g. MinIO.
     * @param endpoint The endpoint of S3.
     * @return The S3 client factory.
     */
    rocksdb::S3ClientFactory BuildS3ClientFactory(const std::string &endpoint);

    /**
     * @brief Initialize the data store.
     * @return True if connect successfully, otherwise false.
     */
    bool Initialize() override;

    /**
     * @brief Open the cloud database.
     * @param cfs_options The cloud file system options.
     * @return True if open successfully, otherwise false.
     */
    bool StartDB(std::string cookie = "",
                 std::string prev_cookie = "") override;

    /**
     * @brief Close the cloud database.
     */
    void Shutdown() override;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param flush_data_req The pointer of the request.
     */
    void FlushData(FlushDataRequest *flush_data_req) override;

    /**
     * @brief Write records to the data store.
     * @param batch_write_req The pointer of the request.
     */
    void DeleteRange(DeleteRangeRequest *delete_range_req) override;

    /**
     * @brief Write records to the data store.
     * @param batch_write_req The pointer of the request.
     */
    void Read(ReadRequest *req) override;

    /**
     * @brief Write records to the data store.
     * @param batch_write_req The pointer of the request.
     */
    void BatchWriteRecords(WriteRecordsRequest *batch_write_req) override;

    /**
     * @brief Create kv table.
     * @param create_table_req The pointer of the request.
     */
    void CreateTable(CreateTableRequest *create_table_req) override;

    /**
     * @brief Drop kv table.
     * @param drop_talbe_req The pointer of the request.
     */
    void DropTable(DropTableRequest *drop_table_req) override;

    /**
     * @brief Scan records from the data store.
     */
    void ScanNext(ScanRequest *scan_req) override;

    /**
     * @brief Close scan operation.
     */
    void ScanClose(ScanRequest *scan_req) override;

    /**
     * @brief Swith this shard of data store to read only mode.
     */
    void SwitchToReadOnly() override;

    /**
     * @brief Swith this shard of data store to read write mode.
     */
    void SwitchToReadWrite() override;

    /**
     * @brief Get the shard status of this data store.
     * @return The shard status.
     */
    DSShardStatus FetchDSShardStatus() const;

protected:
    /**
     * @brief Wait for all pending writes to complete.
     */
    void WaitForPendingWrites();

    /**
     * @brief Increase the pending write counter.
     */
    void IncreaseWriteCounter();

    /**
     * @brief Decrease the pending write counter.
     */
    void DecreaseWriteCounter();

    /**
     * @brief Get the RocksDB pointer.
     */
    rocksdb::DBCloud *GetDBPtr();

private:
    inline std::string MakeCloudManifestCookie(int64_t cc_ng_id, int64_t term);
    inline std::string MakeCloudManifestFile(const std::string &dbname,
                                             int64_t cc_ng_id,
                                             int64_t term);
    inline bool IsCloudManifestFile(const std::string &filename);
    inline std::vector<std::string> SplitString(const std::string &str,
                                                char delimiter);
    inline bool GetCookieFromCloudManifestFile(const std::string &filename,
                                               int64_t &cc_ng_id,
                                               int64_t &term);
    inline int64_t FindMaxTermFromCloudManifestFiles(
        const std::shared_ptr<ROCKSDB_NAMESPACE::CloudStorageProvider>
            &storage_provider,
        const std::string &bucket_prefix,
        const std::string &bucket_name,
        const int64_t cc_ng_id_in_cookie);

private:
    /**
     * @brief Open the cloud database.
     * @param cfs_options The cloud file system options.
     * @return True if open successfully, otherwise false.
     */
    bool OpenCloudDB(const rocksdb::CloudFileSystemOptions &cfs_options);
    rocksdb::InfoLogLevel StringToInfoLogLevel(
        const std::string &log_level_str);

    rocksdb::InfoLogLevel info_log_level_;
    const bool enable_stats_;
    const uint32_t stats_dump_period_sec_;
    const std::string storage_path_;
    const std::string wal_dir_;
    const size_t max_write_buffer_number_;
    const size_t max_background_jobs_;
    const size_t max_background_flushes_;
    const size_t max_background_compactions_;
    const size_t target_file_size_base_;
    const size_t target_file_size_multiplier_;
    const size_t write_buff_size_;
    const bool use_direct_io_for_flush_and_compaction_;
    const bool use_direct_io_for_read_;
    const size_t level0_stop_writes_trigger_;
    const size_t level0_slowdown_writes_trigger_;
    const size_t level0_file_num_compaction_trigger_;
    const size_t max_bytes_for_level_base_;
    const size_t max_bytes_for_level_multiplier_;
    const std::string compaction_style_;
    const size_t soft_pending_compaction_bytes_limit_;
    const size_t hard_pending_compaction_bytes_limit_;
    const size_t max_subcompactions_;
    const size_t write_rate_limit_;
    const size_t batch_write_size_;
    const size_t periodic_compaction_seconds_;
    const std::string dialy_offpeak_time_utc_;
    const std::string db_path_;
    const std::string ckpt_path_;
    const std::string backup_path_;
    const std::string received_snapshot_path_;
    const bool create_db_if_missing_{false};
    bool tx_enable_cache_replacement_{true};
    std::unique_ptr<EloqDS::TTLCompactionFilter> ttl_compaction_filter_{
        nullptr};

    std::unique_ptr<ThreadWorkerPool> query_worker_pool_;
    std::shared_mutex db_mux_;
    std::mutex ddl_mux_;

    const EloqDS::RocksDBCloudConfig cloud_config_;
    rocksdb::CloudFileSystemOptions cfs_options_;
    std::shared_ptr<rocksdb::FileSystem> cloud_fs_{nullptr};
    std::unique_ptr<rocksdb::Env> cloud_env_{nullptr};
    rocksdb::DBCloud *db_{nullptr};

    std::atomic<size_t> ongoing_write_requests_{0};
};

}  // namespace EloqDS
