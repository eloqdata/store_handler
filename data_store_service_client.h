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

#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "eloq_data_store_service/data_store_service.h"
#include "eloq_data_store_service/ds_request.pb.h"
#include "eloq_data_store_service/thread_worker_pool.h"
#include "tx_service/include/cc/cc_shard.h"
#include "tx_service/include/sequences/sequences.h"
#include "tx_service/include/sharder.h"
#include "tx_service/include/store/data_store_handler.h"

namespace EloqDS
{
class DataStoreServiceClient;
class BatchWriteRecordsClosure;
class ReadClosure;
class DeleteRangeClosure;
class FlushDataClosure;
class DropTableClosure;
struct UpsertTableData;
class ScanNextClosure;
class SinglePartitionScanner;

typedef void (*DataStoreCallback)(void *data,
                                  ::google::protobuf::Closure *closure,
                                  DataStoreServiceClient &client,
                                  const remote::CommonResult &result);

class DataStoreServiceClient : public txservice::store::DataStoreHandler
{
public:
    // DataStoreServiceClient();
    ~DataStoreServiceClient();

    DataStoreServiceClient(
        const DataStoreServiceClusterManager &cluster_manager,
        DataStoreService *data_store_service = nullptr)
        : ds_serv_shutdown_indicator_(false),
          cluster_manager_(cluster_manager),
          data_store_service_(data_store_service),
          flying_remote_fetch_count_(0)
    {
        if (data_store_service_ != nullptr)
        {
            data_store_service_->AddListenerForUpdateConfig(
                [this](const DataStoreServiceClusterManager &cluster_manager)
                { this->SetupConfig(cluster_manager); });
        }

        // Add sequence table to pre-built tables
        DLOG(INFO) << "AppendPreBuiltTable: "
                   << txservice::Sequences::table_name_sv_;

        AppendPreBuiltTable(txservice::Sequences::table_name_);
    }

    // The maximum number of retries for RPC requests.
    static const int retry_limit_ = 2;

    /**
     * Connect to remote data store service.
     */
    void SetupConfig(const DataStoreServiceClusterManager &config);

    void ConnectToLocalDataStoreService(
        std::unique_ptr<DataStoreService> ds_serv);

    // ==============================================
    // Group: Functions Inherit from DataStoreHandler
    // ==============================================

    // Override all the virtual functions in DataStoreHandler
    bool Connect() override;

    bool IsSharedStorage() const override
    {
        return true;
    }

    void ScheduleTimerTasks() override;

    /**
     * @brief flush entries in \@param batch to base table or skindex table
     * in data store, stop and return false if node_group is not longer
     * leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param schema_ts
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    bool PutAll(std::unordered_map<
                std::string_view,
                std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
                    &flush_task) override;

    bool NeedPersistKV() override
    {
        return true;
    }

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param
     * batch to base table or skindex table in data store, stop and return
     * false if node_group is not longer leader.
     * @param table_name base table name or sk index name
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    bool PersistKV(const std::vector<std::string> &kv_table_names) override;

    void UpsertTable(
        const txservice::TableSchema *old_table_schema,
        const txservice::TableSchema *new_table_schema,
        txservice::OperationType op_type,
        uint64_t commit_ts,
        txservice::NodeGroupId ng_id,
        int64_t tx_term,
        txservice::CcHandlerResult<txservice::Void> *hd_res,
        const txservice::AlterTableInfo *alter_table_info = nullptr,
        txservice::CcRequestBase *cc_req = nullptr,
        txservice::CcShard *ccs = nullptr,
        txservice::CcErrorCode *err_code = nullptr) override;

    void FetchTableCatalog(const txservice::TableName &ccm_table_name,
                           txservice::FetchCatalogCc *fetch_cc) override;

    void FetchCurrentTableStatistics(
        const txservice::TableName &ccm_table_name,
        txservice::FetchTableStatisticsCc *fetch_cc) override;

    void FetchTableStatistics(
        const txservice::TableName &ccm_table_name,
        txservice::FetchTableStatisticsCc *fetch_cc) override;

    bool UpsertTableStatistics(
        const txservice::TableName &ccm_table_name,
        const std::unordered_map<
            txservice::TableName,
            std::pair<uint64_t, std::vector<txservice::TxKey>>>
            &sample_pool_map,
        uint64_t version) override;

    void FetchTableRanges(txservice::FetchTableRangesCc *fetch_cc) override;

    void FetchRangeSlices(txservice::FetchRangeSlicesReq *fetch_cc) override;

    bool DeleteOutOfRangeData(
        const txservice::TableName &table_name,
        int32_t partition_id,
        const txservice::TxKey *start_key,
        const txservice::TableSchema *table_schema) override;

    bool GetNextRangePartitionId(const txservice::TableName &tablename,
                                 const txservice::TableSchema *table_schema,
                                 uint32_t range_cnt,
                                 int32_t &out_next_partition_id,
                                 int retry_count) override;

    bool Read(const txservice::TableName &table_name,
              const txservice::TxKey &key,
              txservice::TxRecord &rec,
              bool &found,
              uint64_t &version_ts,
              const txservice::TableSchema *table_schema) override;

    DataStoreOpStatus FetchRecord(txservice::FetchRecordCc *fetch_cc) override;

    std::unique_ptr<txservice::store::DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        uint32_t ng_id,
        const txservice::TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const std::vector<txservice::store::DataStoreSearchCond> &search_cond,
        const txservice::KeySchema *key_schema,
        const txservice::RecordSchema *rec_schema,
        const txservice::KVCatalogInfo *kv_info,
        bool scan_foward) override;

    txservice::store::DataStoreHandler::DataStoreOpStatus LoadRangeSlice(
        const txservice::TableName &table_name,
        const txservice::KVCatalogInfo *kv_info,
        uint32_t range_partition_id,
        txservice::FillStoreSliceCc *load_slice_req) override;

    bool UpdateRangeSlices(const txservice::TableName &table_name,
                           uint64_t version,
                           txservice::TxKey range_start_key,
                           std::vector<const txservice::StoreSlice *> slices,
                           int32_t partition_id,
                           uint64_t range_version) override;

    bool UpsertRanges(const txservice::TableName &table_name,
                      std::vector<txservice::SplitRangeInfo> range_info,
                      uint64_t version) override;

    std::string EncodeRangeKey(const txservice::TableName &table_name,
                               const txservice::TxKey &range_start_key);
    std::string EncodeRangeValue(int32_t range_id,
                                 uint64_t range_version,
                                 uint64_t version,
                                 uint32_t segment_cnt);
    std::string EncodeRangeSliceKey(const txservice::TableName &table_name,
                                    int32_t range_id,
                                    uint32_t segment_id);

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found,
                    uint64_t &version_ts) override;

    bool DiscoverAllTableNames(
        std::vector<std::string> &norm_name_vec,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) override;

    //-- database
    bool UpsertDatabase(std::string_view db,
                        std::string_view definition) override;
    bool DropDatabase(std::string_view db) override;
    bool FetchDatabase(
        std::string_view db,
        std::string &definition,
        bool &found,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) override;
    bool FetchAllDatabase(
        std::vector<std::string> &dbnames,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) override;

    bool DropKvTable(const std::string &kv_table_name) override;

    void DropKvTableAsync(const std::string &kv_table_name) override;

    std::string CreateKVCatalogInfo(
        const txservice::TableSchema *table_schema) const override;

    txservice::KVCatalogInfo::uptr DeserializeKVCatalogInfo(
        const std::string &kv_info_str, size_t &offset) const override;

    std::string CreateNewKVCatalogInfo(
        const txservice::TableName &table_name,
        const txservice::TableSchema *current_table_schema,
        txservice::AlterTableInfo &alter_table_info) override;

    /**
     * @brief Write batch historical versions into DataStore.
     *
     */
    bool PutArchivesAll(std::unordered_map<
                        std::string_view,
                        std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
                            &flush_task) override;
    /**
     * @brief Copy record from base/sk table to mvcc_archives.
     */
    bool CopyBaseToArchive(
        std::unordered_map<
            std::string_view,
            std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
            &flush_task) override;

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts)
     * historical version.
     */
    bool FetchVisibleArchive(const txservice::TableName &table_name,
                             const txservice::KVCatalogInfo *kv_info,
                             const txservice::TxKey &key,
                             const uint64_t upper_bound_ts,
                             txservice::TxRecord &rec,
                             txservice::RecordStatus &rec_status,
                             uint64_t &commit_ts) override;

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    bool FetchArchives(const txservice::TableName &table_name,
                       const txservice::KVCatalogInfo *kv_info,
                       const txservice::TxKey &key,
                       std::vector<txservice::VersionTxRecord> &archives,
                       uint64_t from_ts) override;

    bool NeedCopyRange() const override;

    void RestoreTxCache(txservice::NodeGroupId cc_ng_id,
                        int64_t cc_ng_term) override;

    bool OnLeaderStart(uint32_t *next_leader_node) override;

    void OnStartFollowing() override;

    void OnShutdown() override;

    void HandleShardingError(const ::EloqDS::remote::CommonResult &result)
    {
        cluster_manager_.HandleShardingError(result);
    }

    std::shared_ptr<brpc::Channel> GetDataStoreServiceChannelByPartitionId(
        uint32_t partition_id);
    std::shared_ptr<brpc::Channel> UpdateDataStoreServiceChannelByPartitionId(
        uint32_t partition_id);
    std::shared_ptr<brpc::Channel> GetDataStoreServiceChannelByShardId(
        uint32_t shard_id);
    std::shared_ptr<brpc::Channel> UpdateDataStoreServiceChannelByShardId(
        uint32_t shard_id);
    std::shared_ptr<brpc::Channel> GetDataStoreServiceChannel(
        const DSSNode &node);
    std::shared_ptr<brpc::Channel> UpdateDataStoreServiceChannel(
        const DSSNode &node);

    /**
     * Serialize a record with is_deleted flag and record string.
     * @param is_deleted
     * @param rec
     * @return rec_str
     */
    static std::string SerializeTxRecord(bool is_deleted,
                                         const txservice::TxRecord *rec);

    /**
     * Serialize a record with is_deleted flag and record string.
     * @param is_deleted
     * @param rec
     * @return rec_str
     */
    static void SerializeTxRecord(bool is_deleted,
                                  const txservice::TxRecord *rec,
                                  std::vector<uint64_t> &record_tmp_mem_area,
                                  std::vector<std::string_view> &record_parts,
                                  size_t &write_batch_size);

    static void SerializeTxRecord(const txservice::TxRecord *rec,
                                  std::vector<uint64_t> &record_tmp_mem_area,
                                  std::vector<std::string_view> &record_parts,
                                  size_t &write_batch_size);
    /**
     * Get the is_delete flag from the serialized record string with
     * is_deleted flag
     * @param record
     * @param is_deleted
     * @param offset of the start offset of the range record string
     * @return true if Deserialize successfully, false otherwise
     */
    static bool DeserializeTxRecordStr(const std::string_view record,
                                       bool &is_deleted,
                                       size_t &offset);

    static uint32_t HashArchiveKey(const std::string &kv_table_name,
                                   const txservice::TxKey &tx_key);

    // NOTICE: be_commit_ts is the big endian encode value of commit_ts
    static std::string EncodeArchiveKey(std::string_view table_name,
                                        std::string_view key,
                                        uint64_t be_commit_ts);

    // NOTICE: be_commit_ts is the big endian encode value of commit_ts
    static void EncodeArchiveKey(std::string_view table_name,
                                 std::string_view key,
                                 uint64_t &be_commit_ts,
                                 std::vector<std::string_view> &keys,
                                 uint64_t &write_batch_size);

    // NOTICE: be_commit_ts is the big endian encode value of commit_ts
    static bool DecodeArchiveKey(const std::string &archive_key,
                                 std::string &table_name,
                                 txservice::TxKey &key,
                                 uint64_t &be_commit_ts);

    static void EncodeArchiveValue(bool is_deleted,
                                   const txservice::TxRecord *value,
                                   size_t &unpack_info_size,
                                   size_t &encoded_blob_size,
                                   std::vector<std::string_view> &record_parts,
                                   size_t &write_batch_size);

    static void DecodeArchiveValue(const std::string &archive_value,
                                   bool &is_deleted,
                                   size_t &value_offset);

    bool InitPreBuiltTables();
    // call this function before Connect().
    bool AppendPreBuiltTable(const txservice::TableName &table_name)
    {
        pre_built_table_names_.emplace(
            txservice::TableName(
                table_name.String(), table_name.Type(), table_name.Engine()),
            table_name.String());
        return true;
    }

    void UpsertTable(UpsertTableData *table_data);
    bool UpsertCatalog(const txservice::TableSchema *table_schema,
                       uint64_t write_time);
    bool DeleteCatalog(const txservice::TableName &base_table_name,
                       uint64_t write_time);

private:
    int32_t MapKeyHashToPartitionId(const txservice::TxKey &key) const
    {
        return (key.Hash() >> 10) & 0x3FF;
    }

    // =====================================================
    // Group: KV Interface
    // Functions that decide if the request is local or remote
    // =====================================================

    void Read(const std::string_view kv_table_name,
              const uint32_t partition_id,
              const std::string_view key,
              void *callback_data,
              DataStoreCallback callback);

    void ReadInternal(ReadClosure *read_clouse);

    void BatchWriteRecords(
        std::string_view kv_table_name,
        int32_t partition_id,
        std::vector<std::string_view> &&key_parts,
        std::vector<std::string_view> &&record_parts,
        std::vector<uint64_t> &&records_ts,
        std::vector<uint64_t> &&records_ttl,
        std::vector<WriteOpType> &&op_types,
        bool skip_wal,
        void *callback_data,
        DataStoreCallback callback,
        // This is the count of key_parts compose of one key
        const uint16_t key_parts_count = 1,
        // This is the count of record_parts compose of one record
        const uint16_t record_parts_count = 1);

    void BatchWriteRecordsInternal(BatchWriteRecordsClosure *closure);

    /**
     * Delete range and flush data are not frequent calls, all calls are sent
     * with rpc.
     */
    void DeleteRange(const std::string_view table_name,
                     const int32_t partition_id,
                     const std::string &start_key,
                     const std::string &end_key,
                     const bool skip_wal,
                     void *callback_data,
                     DataStoreCallback callback);

    void DeleteRangeInternal(DeleteRangeClosure *delete_range_closure);

    /**
     * Flush data operation guarantees all data in memory is persisted to disk.
     */
    void FlushData(const std::vector<std::string> &kv_table_names,
                   void *callback_data,
                   DataStoreCallback callback);

    void FlushDataInternal(FlushDataClosure *flush_data_closure);

    void ScanNext(const std::string_view table_name,
                  uint32_t partition_id,
                  const std::string_view start_key,
                  const std::string_view end_key,
                  const std::string_view session_id,
                  bool inclusive_start,
                  bool inclusive_end,
                  bool scan_forward,
                  uint32_t batch_size,
                  const std::vector<remote::SearchCondition> *search_conditions,
                  void *callback_data,
                  DataStoreCallback callback);

    void ScanNextInternal(ScanNextClosure *scan_next_closure);

    void ScanClose(const std::string_view table_name,
                   uint32_t partition_id,
                   std::string &session_id,
                   void *callback_data,
                   DataStoreCallback callback);

    void ScanCloseInternal(ScanNextClosure *scan_next_closure);

    /**
     * Drop table in KvStore.
     */
    void DropTable(std::string_view kv_table_name,
                   void *callback_data,
                   DataStoreCallback callback);

    void DropTableInternal(DropTableClosure *flush_data_closure);

    bool CreateKvTable(const std::string &kv_table_name)
    {
        return true;
    }

    bool InitTableRanges(const txservice::TableName &table_name,
                         uint64_t version);

    bool DeleteTableRanges(const txservice::TableName &table_name);

    bool InitTableLastRangePartitionId(const txservice::TableName &table_name);

    bool DeleteTableStatistics(const txservice::TableName &base_table_name);

    // Caculate kv partition id of records in System table(catalogs, ranges,
    // statistics and etc.).
    int32_t KvPartitionIdOf(const txservice::TableName &table) const
    {
#ifdef USE_ONE_ELOQDSS_PARTITION
        return 0;
#else
        std::string_view sv = table.StringView();
        return (std::hash<std::string_view>()(sv)) & 0x3FF;
#endif
    }

    int32_t KvPartitionIdOf(int32_t key_partition,
                            bool is_range_partition = true)
    {
#ifdef USE_ONE_ELOQDSS_PARTITION
        if (is_range_partition)
        {
            return key_partition;
        }
        else
        {
            return 0;
        }
#else
        return key_partition;
#endif
    }

    /**
     * @brief Check if the shard_id is local to the current node.
     * @param shard_id
     * @return true if the shard_id is local to the current node.
     */
    bool IsLocalShard(uint32_t shard_id);

    uint32_t GetShardIdByPartitionId(int32_t partition_id)
    {
        return cluster_manager_.GetShardIdByPartitionId(partition_id);
    }

    /**
     * @brief Check if the partition_id is local to the current node.
     * @param partition_id
     * @return true if the partition_id is local to the current node.
     */
    bool IsLocalPartition(int32_t partition_id);

    bthread::Mutex ds_service_mutex_;
    bthread::ConditionVariable ds_service_cv_;
    std::atomic<bool> ds_serv_shutdown_indicator_;

    // remote data store service configuration
    DataStoreServiceClusterManager cluster_manager_;

    // point to the data store service if it is colocated
    DataStoreService *data_store_service_;

    std::atomic<uint64_t> flying_remote_fetch_count_{0};
    // Work queue for fetch records from primary node
    std::deque<txservice::FetchRecordCc *> remote_fetch_cc_queue_;

    // table names and their kv table names
    std::unordered_map<txservice::TableName, std::string>
        pre_built_table_names_;
    ThreadWorkerPool upsert_table_worker_{1};

    friend class ReadClosure;
    friend class BatchWriteRecordsClosure;
    friend class FlushDataClosure;
    friend class DeleteRangeClosure;
    friend class DropTableClosure;
    friend class ScanNextClosure;
    friend class SinglePartitionScanner;
    friend void FetchAllDatabaseCallback(void *data,
                                         ::google::protobuf::Closure *closure,
                                         DataStoreServiceClient &client,
                                         const remote::CommonResult &result);
    friend void DiscoverAllTableNamesCallback(
        void *data,
        ::google::protobuf::Closure *closure,
        DataStoreServiceClient &client,
        const remote::CommonResult &result);
    friend void FetchTableRangesCallback(void *data,
                                         ::google::protobuf::Closure *closure,
                                         DataStoreServiceClient &client,
                                         const remote::CommonResult &result);
    friend void FetchRangeSlicesCallback(void *data,
                                         ::google::protobuf::Closure *closure,
                                         DataStoreServiceClient &client,
                                         const remote::CommonResult &result);
    friend void FetchTableStatsCallback(void *data,
                                        ::google::protobuf::Closure *closure,
                                        DataStoreServiceClient &client,
                                        const remote::CommonResult &result);
    friend void LoadRangeSliceCallback(void *data,
                                       ::google::protobuf::Closure *closure,
                                       DataStoreServiceClient &client,
                                       const remote::CommonResult &result);
    friend void FetchArchivesCallback(void *data,
                                      ::google::protobuf::Closure *closure,
                                      DataStoreServiceClient &client,
                                      const remote::CommonResult &result);
};

struct UpsertTableData
{
    UpsertTableData() = delete;
    UpsertTableData(const txservice::TableSchema *old_table_schema,
                    const txservice::TableSchema *new_table_schema,
                    txservice::OperationType op_type,
                    uint64_t commit_ts,
                    txservice::NodeGroupId ng_id,
                    int64_t tx_term,
                    txservice::CcHandlerResult<txservice::Void> *hd_res,
                    const txservice::AlterTableInfo *alter_table_info = nullptr,
                    txservice::CcRequestBase *cc_req = nullptr,
                    txservice::CcShard *ccs = nullptr,
                    txservice::CcErrorCode *err_code = nullptr)
        : old_table_schema_(old_table_schema),
          new_table_schema_(new_table_schema),
          op_type_(op_type),
          commit_ts_(commit_ts),
          ng_id_(ng_id),
          tx_term_(tx_term),
          hd_res_(hd_res),
          alter_table_info_(alter_table_info),
          cc_req_(cc_req),
          ccs_(ccs),
          err_code_(err_code)
    {
    }

    ~UpsertTableData() = default;

    void SetFinished()
    {
        if (hd_res_ != nullptr)
        {
            hd_res_->SetFinished();
        }
        else
        {
            assert(cc_req_ != nullptr);
            *err_code_ = txservice::CcErrorCode::NO_ERROR;
            ccs_->Enqueue(cc_req_);
        }
    }

    void SetError(txservice::CcErrorCode err_code)
    {
        if (hd_res_ != nullptr)
        {
            hd_res_->SetError(err_code);
        }
        else
        {
            *err_code_ = err_code;
            ccs_->Enqueue(cc_req_);
        }
    }

    const txservice::TableSchema *old_table_schema_;
    const txservice::TableSchema *new_table_schema_;
    txservice::OperationType op_type_;
    uint64_t commit_ts_;
    txservice::NodeGroupId ng_id_;
    int64_t tx_term_;
    txservice::CcHandlerResult<txservice::Void> *hd_res_;
    const txservice::AlterTableInfo *alter_table_info_;
    txservice::CcRequestBase *cc_req_;
    txservice::CcShard *ccs_;
    txservice::CcErrorCode *err_code_;
};

}  // namespace EloqDS
