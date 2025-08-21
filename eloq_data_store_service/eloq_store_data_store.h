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

#include <unordered_map>
#include <vector>

#include "data_store.h"
#include "data_store_service.h"
#include "eloq_store.h"
#include "kv_options.h"
#include "object_pool.h"

namespace EloqDS
{
class EloqStoreDataStore;

template <typename ReqT>
struct EloqStoreOperationData : public Poolable
{
    EloqStoreOperationData() = default;
    EloqStoreOperationData(const EloqStoreOperationData &rhs) = delete;
    EloqStoreOperationData(EloqStoreOperationData &&rhs) = delete;

    void Reset(Poolable *ds_req_ptr)
    {
        data_store_request_ptr_ = ds_req_ptr;
    }

    void Clear() override
    {
        data_store_request_ptr_->Clear();
        data_store_request_ptr_->Free();
        data_store_request_ptr_ = nullptr;
    }

    ReqT &EloqStoreRequest()
    {
        return eloq_store_request_;
    }

    Poolable *DataStoreRequest() const
    {
        return data_store_request_ptr_;
    }

private:
    ReqT eloq_store_request_;
    Poolable *data_store_request_ptr_{nullptr};
};

struct ScanDeleteOperationData : public Poolable
{
private:
    enum struct Stage
    {
        SCAN = 0,
        DELETE = 1,
    };

public:
    ScanDeleteOperationData() = default;
    ScanDeleteOperationData(const ScanDeleteOperationData &rhs) = delete;
    ScanDeleteOperationData(ScanDeleteOperationData &&rhs) = delete;

    void Reset(Poolable *ds_req_ptr, ::eloqstore::EloqStore *store)
    {
        data_store_request_ptr_ = ds_req_ptr;
        op_ts_ =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        op_stage_ = Stage::SCAN;
        eloq_store_ = store;
    }

    void Clear() override
    {
        data_store_request_ptr_->Clear();
        data_store_request_ptr_->Free();
        data_store_request_ptr_ = nullptr;
        entries_.clear();
    }

    ::eloqstore::ScanRequest &EloqStoreScanRequest()
    {
        return kv_scan_req_;
    }

    ::eloqstore::BatchWriteRequest &EloqStoreWriteRequest()
    {
        return kv_write_req_;
    }

    Poolable *DataStoreRequest() const
    {
        return data_store_request_ptr_;
    }

    const std::string &LastScanEndKey() const
    {
        return last_scan_end_key_;
    }

    void UpdateLastScanEndKey(const std::string &key_str, bool scan_drained)
    {
        last_scan_end_key_ = scan_drained ? "" : key_str;
    }

    uint64_t OpTs() const
    {
        return op_ts_;
    }

    void UpdateOperationStage(Stage next_stage)
    {
        op_stage_ = next_stage;
    }

    Stage OperationStage() const
    {
        return op_stage_;
    }

    ::eloqstore::EloqStore *EloqStoreService() const
    {
        return eloq_store_;
    }

private:
    ::eloqstore::ScanRequest kv_scan_req_;
    ::eloqstore::BatchWriteRequest kv_write_req_;
    Poolable *data_store_request_ptr_{nullptr};
    uint64_t op_ts_{0};
    std::vector<::eloqstore::WriteDataEntry> entries_;
    std::string last_scan_end_key_{""};
    Stage op_stage_{Stage::SCAN};
    ::eloqstore::EloqStore *eloq_store_{nullptr};

    friend class EloqStoreDataStore;
};

struct EloqStoreConfig
{
    // Number of shards (threads).
    uint16_t worker_count_{1};
    // EloqStore storage path.
    std::string storage_path_{""};
    // Max number of open files.
    uint32_t open_files_limit_{1024};
    // Storage path on cloud service.
    std::string cloud_store_path_{""};
    // Number of background file GC threads.
    uint16_t gc_threads_{1};
    // Number of threads used by rclone to upload/download files.
    uint16_t cloud_worker_count_{1};
    uint16_t data_page_restart_interval_{16};
    uint16_t index_page_restart_interval_{16};
    uint32_t init_page_count_{1 << 15};
    // Skip checksum verification when reading pages.
    bool skip_verify_checksum_{false};
    // Max amount of cached index pages per shard.
    uint32_t index_buffer_pool_size_{1 << 15};
    // Limit manifest file size.
    uint32_t manifest_limit_{8 << 20};
    // Size of io-uring submission queue per shard.
    uint32_t io_queue_size_{4096};
    // Max amount of inflight write IO per shard.
    uint32_t max_inflight_write_{64 << 10};
    // The maximum number of pages per batch for the write task.
    uint16_t max_write_batch_pages_{256};
    // Size of io-uring selected buffer ring.
    uint16_t buf_ring_size_{1 << 12};
    // Size of coroutine stack.
    uint32_t coroutine_stack_size_{32 * 1024};
    // Limit number of retained archives.
    uint16_t num_retained_archives_{0};
    // Set the (minimum) archive time interval in seconds.
    uint32_t archive_interval_secs_{86400};
    // The maximum number of running archive tasks at the same time.
    uint16_t max_archive_tasks_{256};
    // Move pages in data file that space amplification factor
    // bigger than this value.
    uint8_t file_amplify_factor_{4};
    // Limit total size of local files per shard. Only take effect when cloud
    // store is enabled.
    size_t local_space_limit_{1ULL << 40};
    // Reserved space ratio for new created/download files. Only take effect
    // when cloud store is enabled.
    uint16_t reserve_space_ratio_{100};
    // Size of B+Tree index/data node (page). Ensure that it is aligned to the
    // system's page size.
    uint16_t data_page_size_{1 << 12};
    // Amount of pages per data file (1 << pages_per_file_shift).
    uint8_t pages_per_file_shift_{11};
    // Amount of pointers stored in overflow page. The maximum can be set to
    // 128 (max_overflow_pointers).
    uint8_t overflow_pointers_{16};
    // Write data file pages in append only mode.
    bool data_append_mode_{false};
    const ::eloqstore::Comparator *comparator_{
        ::eloqstore::Comparator::DefaultComparator()};
};

class EloqStoreDataStore : public DataStore
{
public:
    EloqStoreDataStore(uint32_t shard_id,
                       DataStoreService *data_store_service,
                       const eloqstore::KvOptions &configs);

    ~EloqStoreDataStore() = default;

    bool Initialize() override
    {
        return true;
    }

    bool StartDB(std::string cookie = "", std::string prev_cookie = "") override
    {
        ::eloqstore::KvError res = eloq_store_service_.Start();
        if (res != ::eloqstore::KvError::NoError)
        {
            LOG(ERROR) << "EloqStore start failed with error code: "
                       << static_cast<uint32_t>(res);
        }
        return res == ::eloqstore::KvError::NoError;
    }

    void Shutdown() override
    {
        eloq_store_service_.Stop();
    }

    /**
     * @brief Read record from data store.
     * @param read_req The pointer of the request.
     */
    void Read(ReadRequest *read_req) override;

    /**
     * @brief flush entries in \@param batch to base table or skindex table in
     * data store, stop and return false if node_group is not longer leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    void BatchWriteRecords(WriteRecordsRequest *batch_write_req) override;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param flush_data_req The pointer of the request.
     */
    void FlushData(FlushDataRequest *flush_data_req) override;

    /**
     * @brief Delete records in a range from data store.
     * @param delete_range_req The pointer of the request.
     */
    void DeleteRange(DeleteRangeRequest *delete_range_req) override;

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
     * @brief Fetch next scan result.
     * @param scan_req Scan request.
     */
    void ScanNext(ScanRequest *scan_req) override;

    /**
     * @brief Close scan operation.
     * @param req_shard_id Requested shard id.
     */
    void ScanClose(ScanRequest *scan_req) override;

    /**
     * @brief Switch the data store to read only mode.
     */
    void SwitchToReadOnly() override;

    /**
     * @brief Switch the data store to read write mode.
     */
    void SwitchToReadWrite() override;

private:
    static void OnRead(::eloqstore::KvRequest *req);
    static void OnBatchWrite(::eloqstore::KvRequest *req);
    static void OnDeleteRange(::eloqstore::KvRequest *req);
    static void OnScanNext(::eloqstore::KvRequest *req);
    static void OnScanDelete(::eloqstore::KvRequest *req);
    static void OnFloor(::eloqstore::KvRequest *req);

    void ScanDelete(DeleteRangeRequest *delete_range_req);
    void Floor(ScanRequest *scan_req);

    ::eloqstore::EloqStore eloq_store_service_;
};
}  // namespace EloqDS
