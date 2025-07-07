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
#include "data_store_service_client.h"

#include <glog/logging.h>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service_client_closure.h"
#include "data_store_service_scanner.h"
#include "eloq_data_store_service/object_pool.h"  // ObjectPool
#include "eloq_data_store_service/thread_worker_pool.h"
#include "metrics.h"
#include "partition.h"
#include "store_util.h"  // host_to_big_endian
#include "tx_service/include/cc/local_cc_shards.h"
#include "tx_service/include/error_messages.h"
#include "tx_service/include/sequences/sequences.h"

namespace EloqDS
{

thread_local ObjectPool<BatchWriteRecordsClosure> batch_write_closure_pool_;
thread_local ObjectPool<FlushDataClosure> flush_data_closure_pool_;
thread_local ObjectPool<DeleteRangeClosure> delete_range_closure_pool_;
thread_local ObjectPool<ReadClosure> read_closure_pool_;
thread_local ObjectPool<DropTableClosure> drop_table_closure_pool_;
thread_local ObjectPool<ScanNextClosure> scan_next_closure_pool_;
thread_local ObjectPool<LoadRangeSliceCallbackData>
    load_range_slice_callback_data_pool_;

static const uint64_t MAX_WRITE_BATCH_SIZE = 1024 * 1024;  // 1MB

static const std::string_view kv_cluster_config_name("cluster_config");
static const std::string_view kv_table_catalogs_name("table_catalogs");
static const std::string_view kv_database_catalogs_name("db_catalogs");
static const std::string_view kv_range_table_name("table_ranges");
static const std::string_view kv_range_slices_table_name("table_range_slices");
static const std::string_view kv_last_range_id_name(
    "table_last_range_partition_id");
static const std::string_view kv_table_statistics_name("table_statistics");
static const std::string_view kv_table_statistics_version_name(
    "table_statistics_version");
static const std::string_view kv_mvcc_archive_name("mvcc_archives");
static const std::string_view KEY_SEPARATOR("\\");

DataStoreServiceClient::~DataStoreServiceClient()
{
    {
        std::unique_lock<bthread::Mutex> lk(ds_service_mutex_);
        ds_serv_shutdown_indicator_.store(true, std::memory_order_release);
        ds_service_cv_.notify_all();
        LOG(INFO) << "Notify ds_serv_shutdown_indicator";
    }

    upsert_table_worker_.Shutdown();
}

void DataStoreServiceClient::SetupConfig(
    const DataStoreServiceClusterManager &cluster_manager)
{
    for (const auto &[_, group] : cluster_manager.GetAllShards())
    {
        for (const auto &node : group.nodes_)
        {
            LOG(INFO) << "Node Hostname: " << node.host_name_
                      << ", Port: " << node.port_;
        }
    }
    cluster_manager_ = cluster_manager;
}

bool DataStoreServiceClient::Connect()
{
    bool succeed = false;
    for (int retry = 1; retry <= 5 && !succeed; retry++)
    {
        if (!InitPreBuiltTables())
        {
            succeed = false;
            bthread_usleep(1000000);
        }
        else
        {
            succeed = true;
        }
    }
    return succeed;
}

void DataStoreServiceClient::ScheduleTimerTasks()
{
    LOG(ERROR) << "ScheduleTimerTasks not implemented";
    assert(false);
}

bool DataStoreServiceClient::PutAll(std::vector<txservice::FlushRecord> &batch,
                                    const txservice::TableName &table_name,
                                    const txservice::TableSchema *table_schema,
                                    uint32_t node_group)
{
    const std::string &kv_table_name =
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);

    std::vector<std::string_view> key_parts;
    std::vector<std::string_view> record_parts;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    size_t write_batch_size = 0;
    uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();

    if (table_name.Engine() == txservice::TableEngine::EloqKv)
    {
        std::unordered_map<int32_t, std::vector<uint32_t>> partitions_map;
        // Group the records by the partition id.
        size_t reserve_size = batch.size() / 1024 * 2;
        for (size_t i = 0; i < batch.size(); ++i)
        {
            int32_t kv_partition_id =
                KvPartitionIdOf(batch[i].partition_id_, false);
            auto [it, inserted] = partitions_map.try_emplace(kv_partition_id);
            if (inserted)
            {
                it->second.reserve(reserve_size);
            }
            it->second.emplace_back(i);
        }

        // Send the batch request
        SyncPutAllData sync_putall;
        uint32_t batch_cnt = 0;
        for (auto part_it = partitions_map.begin();
             part_it != partitions_map.end();
             ++part_it)
        {
            auto &flush_recs = part_it->second;
            size_t recs_cnt = flush_recs.size();
            key_parts.reserve(recs_cnt);
            record_parts.reserve(recs_cnt);
            records_ts.reserve(recs_cnt);
            records_ttl.reserve(recs_cnt);
            op_types.reserve(recs_cnt);
            for (auto idx : flush_recs)
            {
                txservice::FlushRecord &ckpt_rec = batch.at(idx);
                txservice::TxKey tx_key = ckpt_rec.Key();

                assert(ckpt_rec.partition_id_ < 1024);
                // Start a new batch if done with current partition.
                if (write_batch_size >= MAX_WRITE_BATCH_SIZE)
                {
                    BatchWriteRecords(kv_table_name,
                                      part_it->first,
                                      std::move(key_parts),
                                      std::move(record_parts),
                                      std::move(records_ts),
                                      std::move(records_ttl),
                                      std::move(op_types),
                                      true,
                                      &sync_putall,
                                      SyncPutAllCallback);
                    write_batch_size = 0;
                    ++batch_cnt;
                }

                assert(ckpt_rec.payload_status_ ==
                           txservice::RecordStatus::Normal ||
                       ckpt_rec.payload_status_ ==
                           txservice::RecordStatus::Deleted);

                uint64_t ttl =
                    ckpt_rec.payload_status_ == txservice::RecordStatus::Normal
                        ? ckpt_rec.Payload()->GetTTL()
                        : 0;
                if (ckpt_rec.payload_status_ ==
                        txservice::RecordStatus::Normal &&
                    (!ckpt_rec.Payload()->HasTTL() || ttl > now))
                {
                    key_parts.emplace_back(
                        std::string_view(tx_key.Data(), tx_key.Size()));
                    write_batch_size += tx_key.Size();

                    const txservice::TxRecord *rec = ckpt_rec.Payload();
                    // Upserts a key to the k-v store
                    record_parts.emplace_back(std::string_view(
                        rec->EncodedBlobData(), rec->EncodedBlobSize()));
                    write_batch_size += rec->EncodedBlobSize();

                    records_ts.push_back(ckpt_rec.commit_ts_);
                    write_batch_size += sizeof(uint64_t);  // commit_ts
                                                           //
                    records_ttl.push_back(ttl);
                    write_batch_size += sizeof(uint64_t);  // ttl

                    op_types.push_back(WriteOpType::PUT);
                    write_batch_size += sizeof(WriteOpType);
                }
                else
                {
                    key_parts.emplace_back(
                        std::string_view(tx_key.Data(), tx_key.Size()));
                    write_batch_size += tx_key.Size();

                    record_parts.emplace_back(std::string_view());

                    records_ts.push_back(ckpt_rec.commit_ts_);
                    write_batch_size += sizeof(uint64_t);  // commit_ts

                    records_ttl.push_back(0);              // no ttl
                    write_batch_size += sizeof(uint64_t);  // ttl

                    op_types.push_back(WriteOpType::DELETE);
                    write_batch_size += sizeof(WriteOpType);
                }
            }
            // Send out the last batch of this partition
            if (key_parts.size() > 0)
            {
                BatchWriteRecords(kv_table_name,
                                  part_it->first,
                                  std::move(key_parts),
                                  std::move(record_parts),
                                  std::move(records_ts),
                                  std::move(records_ttl),
                                  std::move(op_types),
                                  true,
                                  &sync_putall,
                                  SyncPutAllCallback);
                write_batch_size = 0;
                ++batch_cnt;
            }
        }

        // Wait the result.
        {
            std::unique_lock<bthread::Mutex> lk(sync_putall.mux_);
            sync_putall.unfinished_request_cnt_ += batch_cnt;
            sync_putall.all_request_started_ = true;
            while (sync_putall.unfinished_request_cnt_ != 0)
            {
                sync_putall.cv_.wait(lk);
            }
        }

        if (sync_putall.result_.error_code() !=
            remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING)
                << "DataStoreHandler: Failed to write batch for table: "
                << kv_table_name << ", with error code: "
                << static_cast<uint32_t>(sync_putall.result_.error_code());
            return false;
        }
    }
    else
    {
        uint16_t parts_cnt_per_key = 1;
        uint16_t parts_cnt_per_record = 5;
        int32_t last_partition_id = batch[0].partition_id_;
        SyncCallbackData callback_data;
        // The ttl for deleted record, current time plus 24 hours
        uint64_t retired_ttl_for_deleted = now + 24 * 60 * 60 * 1000;
        std::vector<size_t> record_tmp_mem_area;
        record_tmp_mem_area.reserve(MAX_WRITE_BATCH_SIZE);

        size_t flush_idx = 0;
        for (; flush_idx < batch.size(); ++flush_idx)
        {
            txservice::FlushRecord &ckpt_rec = batch.at(flush_idx);

            txservice::TxKey tx_key = ckpt_rec.Key();

            // Start a new batch if done with current partition.
            if (ckpt_rec.partition_id_ != last_partition_id ||
                write_batch_size >= MAX_WRITE_BATCH_SIZE)
            {
                callback_data.Reset();

                BatchWriteRecords(kv_table_name,
                                  KvPartitionIdOf(last_partition_id, true),
                                  std::move(key_parts),
                                  std::move(record_parts),
                                  std::move(records_ts),
                                  std::move(records_ttl),
                                  std::move(op_types),
                                  true,
                                  &callback_data,
                                  &SyncCallback,
                                  parts_cnt_per_key,
                                  parts_cnt_per_record);
                callback_data.Wait();
                write_batch_size = 0;
                record_tmp_mem_area.clear();
                if (callback_data.Result().error_code() !=
                    EloqDS::remote::DataStoreError::NO_ERROR)
                {
                    LOG(WARNING) << "DataStoreHandler: Failed to write batch.";

                    return false;
                }

                if (txservice::Sharder::Instance().LeaderTerm(node_group) < 0)
                {
                    LOG(WARNING) << "DataStoreHandler: PutAll is broken for "
                                    "leader term changed.";
                    return false;
                }
            }

            last_partition_id = ckpt_rec.partition_id_;

            assert(
                ckpt_rec.payload_status_ == txservice::RecordStatus::Normal ||
                ckpt_rec.payload_status_ == txservice::RecordStatus::Deleted);

            bool is_deleted =
                !(ckpt_rec.payload_status_ == txservice::RecordStatus::Normal);
            key_parts.emplace_back(
                std::string_view(tx_key.Data(), tx_key.Size()));
            write_batch_size += tx_key.Size();

            const txservice::TxRecord *rec = ckpt_rec.Payload();
            // encode is_delete, encoded_blob_data and unpack_info
            if (is_deleted)
            {
                records_ttl.push_back(retired_ttl_for_deleted);
            }
            else
            {
                records_ttl.push_back(0);  // no ttl
            }
            write_batch_size += sizeof(uint64_t);  // ttl

            op_types.push_back(WriteOpType::PUT);
            write_batch_size += sizeof(WriteOpType);

            SerializeTxRecord(is_deleted,
                              rec,
                              record_tmp_mem_area,
                              record_parts,
                              write_batch_size);

            records_ts.push_back(ckpt_rec.commit_ts_);
            write_batch_size += sizeof(uint64_t);
        }

        // Send out the last batch
        if (key_parts.size() > 0)
        {
            callback_data.Reset();

            BatchWriteRecords(kv_table_name,
                              KvPartitionIdOf(last_partition_id, true),
                              std::move(key_parts),
                              std::move(record_parts),
                              std::move(records_ts),
                              std::move(records_ttl),
                              std::move(op_types),
                              true,
                              &callback_data,
                              &SyncCallback,
                              parts_cnt_per_key,
                              parts_cnt_per_record);
            callback_data.Wait();
            record_tmp_mem_area.clear();
            write_batch_size = 0;
            if (callback_data.Result().error_code() !=
                EloqDS::remote::DataStoreError::NO_ERROR)
            {
                LOG(WARNING) << "DataStoreHandler: Failed to write batch.";

                return false;
            }
        }
    }

    if (txservice::Sharder::Instance().LeaderTerm(node_group) < 0)
    {
        LOG(WARNING) << "DataStoreHandler: leader transferred of ng#"
                     << node_group;
        return false;
    }

    return true;
}

// TODO(lzx): change DataStoreHander::PersistKV(), only need KVCatalogInfo
// instead of TableSchema. why "node_group" and "version" are not used now ?
bool DataStoreServiceClient::PersistKV(
    const std::vector<std::string> &kv_table_names)
{
    SyncCallbackData callback_data;

    FlushData(kv_table_names, &callback_data, &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "DataStoreHandler: Failed to do PersistKV. Error: "
                     << callback_data.Result().error_msg();
        return false;
    }
    DLOG(INFO) << "DataStoreHandler::PersistKV success.";

    return true;
}

void DataStoreServiceClient::UpsertTable(
    const txservice::TableSchema *old_table_schema,
    const txservice::TableSchema *new_table_schema,
    txservice::OperationType op_type,
    uint64_t commit_ts,
    txservice::NodeGroupId ng_id,
    int64_t tx_term,
    txservice::CcHandlerResult<txservice::Void> *hd_res,
    const txservice::AlterTableInfo *alter_table_info,
    txservice::CcRequestBase *cc_req,
    txservice::CcShard *ccs,
    txservice::CcErrorCode *err_code)
{
    int64_t leader_term =
        txservice::Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (leader_term < 0)
    {
        hd_res->SetError(txservice::CcErrorCode::TX_NODE_NOT_LEADER);
        return;
    }

    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *)
        { txservice::Sharder::Instance().UnpinNodeGroupData(ng_id); });

    if (leader_term != tx_term)
    {
        hd_res->SetError(txservice::CcErrorCode::NG_TERM_CHANGED);
        return;
    }

    // Use old schema for drop table as the new schema would be null.
    UpsertTableData *table_data = new UpsertTableData(old_table_schema,
                                                      new_table_schema,
                                                      op_type,
                                                      commit_ts,
                                                      ng_id,
                                                      tx_term,
                                                      hd_res,
                                                      alter_table_info,
                                                      cc_req,
                                                      ccs,
                                                      err_code);

    upsert_table_worker_.SubmitWork([this, table_data]()
                                    { this->UpsertTable(table_data); });
}

void DataStoreServiceClient::FetchTableCatalog(
    const txservice::TableName &ccm_table_name,
    txservice::FetchCatalogCc *fetch_cc)
{
    int32_t kv_partition_id = 0;
    std::string_view key = fetch_cc->CatalogName().StringView();
    Read(kv_table_catalogs_name,
         kv_partition_id,
         key,
         fetch_cc,
         &FetchTableCatalogCallback);
}

void DataStoreServiceClient::FetchCurrentTableStatistics(
    const txservice::TableName &ccm_table_name,
    txservice::FetchTableStatisticsCc *fetch_cc)
{
    std::string_view sv = ccm_table_name.StringView();
    int32_t kv_partition_id = KvPartitionIdOf(ccm_table_name);

    fetch_cc->SetStoreHandler(this);
    Read(kv_table_statistics_version_name,
         kv_partition_id,
         sv,
         fetch_cc,
         &FetchCurrentTableStatsCallback);
}

void DataStoreServiceClient::FetchTableStatistics(
    const txservice::TableName &ccm_table_name,
    txservice::FetchTableStatisticsCc *fetch_cc)
{
    uint64_t version = fetch_cc->CurrentVersion();
    uint64_t be_version = EloqShare::host_to_big_endian(version);
    std::string start_key;
    start_key.append(ccm_table_name.StringView());
    start_key.append(reinterpret_cast<const char *>(&be_version),
                     sizeof(uint64_t));
    std::string end_key = start_key;
    end_key.back()++;

    int32_t partition_id = KvPartitionIdOf(ccm_table_name);

    FetchTableStatsCallbackData *callback_data =
        new FetchTableStatsCallbackData(fetch_cc,
                                        kv_table_statistics_name,
                                        partition_id,
                                        std::move(start_key),
                                        std::move(end_key));
    // NOTICE: here batch_size is 1, because the size of item in
    // {kv_table_statistics_name} may be more than MAX_WRITE_BATCH_SIZE.
    ScanNext(callback_data->kv_table_name_,
             callback_data->partition_id_,
             callback_data->start_key_,
             callback_data->end_key_,
             callback_data->session_id_,
             false,
             false,
             true,
             1,
             &callback_data->search_conditions_,
             callback_data,
             &FetchTableStatsCallback);
}

// Each node group contains a sample pool, when write them to storage,
// we merge them together. The merged sample pool may be too large to store
// in one row. Therefore, we have to store table statistics segmentally.
//
// (1) We store sample keys of table statistics in
// {kv_table_statistics_name} table using the following format:
//
// segment_key: [table_name + version + segment_id + index_name];
// segment_record: [index_type + records_count + (key_size +
// key) + (key_size + key) + ... ];
//
// (2) We store the ckpt version of each table  statistics version in
// {kv_table_statistics_version_name} table using the following format:
//
// key: [table_name]; record: [ckpt_version];

std::string EncodeTableStatsKey(const txservice::TableName &base_table_name,
                                const txservice::TableName &index_name,
                                uint64_t version,
                                uint32_t segment_id)
{
    std::string key;
    std::string_view table_sv = base_table_name.StringView();
    std::string_view index_sv = index_name.StringView();
    uint64_t be_version = EloqShare::host_to_big_endian(version);
    uint32_t be_segment_id = EloqShare::host_to_big_endian(segment_id);

    key.reserve(table_sv.size() + sizeof(be_version) + sizeof(be_segment_id) +
                index_sv.size());

    key.append(table_sv);
    key.append(reinterpret_cast<const char *>(&be_version), sizeof(uint64_t));
    key.append(reinterpret_cast<const char *>(&be_segment_id),
               sizeof(uint32_t));
    key.append(index_sv);
    return key;
}

bool DataStoreServiceClient::UpsertTableStatistics(
    const txservice::TableName &ccm_table_name,
    const std::unordered_map<txservice::TableName,
                             std::pair<uint64_t, std::vector<txservice::TxKey>>>
        &sample_pool_map,
    uint64_t version)
{
    // 1- split the sample keys into segments

    std::vector<std::string> segment_keys;
    std::vector<std::string> segment_records;

    for (const auto &[indexname, sample_pool] : sample_pool_map)
    {
        uint64_t records_count = sample_pool.first;
        auto &sample_keys = sample_pool.second;

        uint32_t segment_id = 0;
        std::string segment_key =
            EncodeTableStatsKey(ccm_table_name, indexname, version, segment_id);
        size_t batch_size = segment_key.size();

        std::string segment_record;
        segment_record.reserve(MAX_WRITE_BATCH_SIZE - batch_size);
        // index-type
        uint8_t index_type_int = static_cast<uint8_t>(indexname.Type());
        segment_record.append(reinterpret_cast<const char *>(&index_type_int),
                              sizeof(uint8_t));
        // records-count
        segment_record.append(reinterpret_cast<const char *>(&records_count),
                              sizeof(uint64_t));

        for (size_t i = 0; i < sample_keys.size(); ++i)
        {
            uint32_t key_size = sample_keys[i].Size();
            segment_record.append(reinterpret_cast<const char *>(&key_size),
                                  sizeof(uint32_t));
            batch_size += sizeof(uint32_t);
            segment_record.append(sample_keys[i].Data(), sample_keys[i].Size());
            batch_size += key_size;

            if (batch_size >= MAX_WRITE_BATCH_SIZE)
            {
                segment_keys.emplace_back(std::move(segment_key));
                segment_records.emplace_back(std::move(segment_record));
                // segment_size = 0;
                ++segment_id;

                segment_key = EncodeTableStatsKey(
                    ccm_table_name, indexname, version, segment_id);

                batch_size = segment_key.size();

                segment_record.clear();
                segment_record.reserve(MAX_WRITE_BATCH_SIZE - batch_size);
                // index-type
                uint8_t index_type_int = static_cast<uint8_t>(indexname.Type());
                segment_record.append(
                    reinterpret_cast<const char *>(&index_type_int),
                    sizeof(uint8_t));
                // records-count
                segment_record.append(
                    reinterpret_cast<const char *>(&records_count),
                    sizeof(uint64_t));
            }
        }

        if (segment_record.size() > 0)
        {
            segment_keys.emplace_back(std::move(segment_key));
            segment_records.emplace_back(std::move(segment_record));
        }
    }

    // 2- write the segments to storage
    int32_t kv_partition_id = KvPartitionIdOf(ccm_table_name);
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;

    for (size_t i = 0; i < segment_keys.size(); ++i)
    {
        keys.emplace_back(segment_keys[i]);
        records.emplace_back(segment_records[i]);
        records_ts.emplace_back(version);
        records_ttl.emplace_back(0);  // no ttl
        op_types.emplace_back(WriteOpType::PUT);

        // For segments are splitted based on MAX_WRITE_BATCH_SIZE, execute
        // one write request for each segment record.

        callback_data.Reset();
        BatchWriteRecords(kv_table_statistics_name,
                          kv_partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          true,
                          &callback_data,
                          &SyncCallback);
        callback_data.Wait();

        if (callback_data.Result().error_code() !=
            EloqDS::remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING) << "UpdatetableStatistics: Failed to write segments.";

            return false;
        }
    }

    // 3- Update the ckpt version of the table statistics
    callback_data.Reset();
    keys.emplace_back(ccm_table_name.StringView());
    std::string version_str = std::to_string(version);
    records.emplace_back(version_str);
    records_ts.emplace_back(version);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);
    BatchWriteRecords(kv_table_statistics_version_name,
                      kv_partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      true,
                      &callback_data,
                      &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpdatetableStatistics: Failed to write segments.";

        return false;
    }

    // 4- Delete old version data of the table statistics
    uint64_t version0 = 0;
    std::string start_key = ccm_table_name.String();
    // The big endian and small endian encoding of 0 is same.
    start_key.append(reinterpret_cast<const char *>(&version0),
                     sizeof(uint64_t));

    std::string end_key = ccm_table_name.String();
    uint64_t be_version = EloqShare::host_to_big_endian(version);
    end_key.append(reinterpret_cast<const char *>(&be_version),
                   sizeof(uint64_t));

    callback_data.Reset();
    DeleteRange(kv_table_statistics_name,
                kv_partition_id,
                start_key,
                end_key,
                true,
                &callback_data,
                &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpdatetableStatistics: Failed to write ckpt version.";
        return false;
    }

    return true;
}

void DataStoreServiceClient::FetchTableRanges(
    txservice::FetchTableRangesCc *fetch_cc)
{
    int32_t kv_partition_id = KvPartitionIdOf(fetch_cc->table_name_);

    std::string start_key = fetch_cc->table_name_.String();
    std::string end_key = start_key;
    end_key.back()++;

    FetchTableRangesCallbackData *callback_data =
        new FetchTableRangesCallbackData(kv_range_table_name,
                                         fetch_cc,
                                         kv_partition_id,
                                         100,
                                         std::move(start_key),
                                         std::move(end_key));

    ScanNext(callback_data->kv_table_name_,
             callback_data->partition_id_,
             callback_data->start_key_,
             callback_data->end_key_,
             callback_data->session_id_,
             true,
             false,
             true,
             callback_data->batch_size_,
             &callback_data->search_conds_,
             callback_data,
             &FetchTableRangesCallback);
}

void DataStoreServiceClient::FetchRangeSlices(
    txservice::FetchRangeSlicesReq *fetch_cc)
{
    // 1- fetch range info from {kv_range_table_name}
    // 2- fetch range slices from {kv_range_slices_table_name}

    if (txservice::Sharder::Instance().TryPinNodeGroupData(
            fetch_cc->cc_ng_id_) != fetch_cc->cc_ng_term_)
    {
        fetch_cc->SetFinish(txservice::CcErrorCode::NG_TERM_CHANGED);
        return;
    }
    int32_t kv_partition_id = KvPartitionIdOf(fetch_cc->table_name_);

    txservice::TxKey start_key =
        fetch_cc->range_entry_->GetRangeInfo()->StartTxKey();
    std::string range_key = EncodeRangeKey(fetch_cc->table_name_, start_key);
    FetchRangeSlicesCallbackData *callback_data =
        new FetchRangeSlicesCallbackData(kv_range_table_name,
                                         kv_range_slices_table_name,
                                         fetch_cc,
                                         kv_partition_id,
                                         std::move(range_key));

    Read(callback_data->kv_range_table_name_,
         callback_data->kv_partition_id_,
         callback_data->key_,
         callback_data,
         &FetchRangeSlicesCallback);
}

bool DataStoreServiceClient::DeleteOutOfRangeData(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const txservice::TxKey *start_key,
    const txservice::TableSchema *table_schema)
{
    const std::string &kv_table_name =
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);
    std::string start_key_str;
    if (start_key == txservice::TxKeyFactory::NegInfTxKey())
    {
        const txservice::TxKey *neg_key =
            txservice::TxKeyFactory::PackedNegativeInfinity();
        start_key_str = std::string(neg_key->Data(), neg_key->Size());
    }
    else
    {
        start_key_str = std::string(start_key->Data(), start_key->Size());
    }

    std::string end_key_str = "";

    SyncCallbackData callback_data;
    DeleteRange(kv_table_name,
                KvPartitionIdOf(partition_id, true),
                start_key_str,
                end_key_str,
                false,
                &callback_data,
                &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DataStoreHandler: Failed to do DeleteOutOfRangeData. "
                   << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

// TODO(lzx): remove this function from DataStoreHandler after updating
// other store_handler (fetch range partition id from sequences table).
bool DataStoreServiceClient::GetNextRangePartitionId(
    const txservice::TableName &tablename,
    const txservice::TableSchema *table_schema,
    uint32_t range_cnt,
    int32_t &out_next_partition_id,
    int retry_count)
{
    int64_t reserved_cnt;
    uint64_t key_schema_ts;
    if (tablename.IsBase())
    {
        key_schema_ts = table_schema->KeySchema()->SchemaTs();
    }
    else
    {
        key_schema_ts = table_schema->IndexKeySchema(tablename)->SchemaTs();
    }

    int64_t first_reserved_id = -1;
    bool res = txservice::Sequences::ApplyIdOfTableRangePartition(
        tablename, range_cnt, first_reserved_id, reserved_cnt, key_schema_ts);

    if (!res)
    {
        LOG(ERROR) << "GetNextRangePartitionId failed for not assigned "
                      "enough range ids, first_reserved_id:"
                   << first_reserved_id << ", reserved_cnt: " << reserved_cnt;
        return false;
    }
    assert(reserved_cnt == range_cnt);

    assert(first_reserved_id + reserved_cnt < INT32_MAX);

    out_next_partition_id = static_cast<int32_t>(first_reserved_id);
    return true;
}

bool DataStoreServiceClient::Read(const txservice::TableName &table_name,
                                  const txservice::TxKey &key,
                                  txservice::TxRecord &rec,
                                  bool &found,
                                  uint64_t &version_ts,
                                  const txservice::TableSchema *table_schema)
{
    LOG(ERROR) << "Read not implemented";
    return true;
}

std::unique_ptr<txservice::store::DataStoreScanner>
DataStoreServiceClient::ScanForward(
    const txservice::TableName &table_name,
    uint32_t ng_id,
    const txservice::TxKey &start_key,
    bool inclusive,
    uint8_t key_parts,
    const std::vector<txservice::store::DataStoreSearchCond> &search_cond,
    const txservice::KeySchema *key_schema,
    const txservice::RecordSchema *rec_schema,
    const txservice::KVCatalogInfo *kv_info,
    bool scan_forward)
{
    if (scan_forward)
    {
        auto scanner =
            std::make_unique<DataStoreServiceHashPartitionScanner<true>>(
                this,
                key_schema,
                rec_schema,
                table_name,
                kv_info,
                start_key,
                inclusive,
                search_cond,
                100);

        // Call Init() before returning the scanner
        scanner->Init();

        return scanner;
    }
    else
    {
        auto scanner =
            std::make_unique<DataStoreServiceHashPartitionScanner<false>>(
                this,
                key_schema,
                rec_schema,
                table_name,
                kv_info,
                start_key,
                inclusive,
                search_cond,
                100);

        // Call Init() before returning the scanner
        scanner->Init();

        return scanner;
    }
}

txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::LoadRangeSlice(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    uint32_t range_partition_id,
    txservice::FillStoreSliceCc *load_slice_req)
{
    int64_t leader_term = txservice::Sharder::Instance().TryPinNodeGroupData(
        load_slice_req->NodeGroup());
    if (leader_term < 0)
    {
        return txservice::store::DataStoreHandler::DataStoreOpStatus::Error;
    }

    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id = load_slice_req->NodeGroup()](void *)
        { txservice::Sharder::Instance().UnpinNodeGroupData(ng_id); });
    std::string start_key_str;
    const txservice::TxKey &start_key = load_slice_req->StartKey();
    if (start_key == *txservice::TxKeyFactory::NegInfTxKey())
    {
        const txservice::TxKey *neg_key =
            txservice::TxKeyFactory::PackedNegativeInfinity();
        start_key_str = std::string(neg_key->Data(), neg_key->Size());
    }
    else
    {
        start_key_str = std::string(start_key.Data(), start_key.Size());
    }

    std::string end_key_str;
    const txservice::TxKey &end_key = load_slice_req->EndKey();
    if (end_key == *txservice::TxKeyFactory::PosInfTxKey())
    {
        // end_key of empty string indicates the positive infinity in the
        // ScanNext
        end_key_str = "";
    }
    else
    {
        end_key_str = std::string(end_key.Data(), end_key.Size());
    }

    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    LoadRangeSliceCallbackData *callback_data =
        load_range_slice_callback_data_pool_.NextObject();
    int32_t kv_partition_id = KvPartitionIdOf(range_partition_id, true);
    callback_data->Reset(kv_table_name,
                         kv_partition_id,
                         load_slice_req,
                         std::move(start_key_str),
                         std::move(end_key_str),
                         "",  // session_id
                         1000,
                         defer_unpin);

    ScanNext(kv_info->GetKvTableName(table_name),
             kv_partition_id,
             callback_data->last_key_,
             callback_data->end_key_,
             "",                          // session_id
             true,                        // include start_key
             false,                       // include end_key
             true,                        // scan forward
             callback_data->batch_size_,  // batch size
             nullptr,                     // search condition
             callback_data,
             &LoadRangeSliceCallback);

    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

// Range contains two parts info : range and slices.
// Then we store the range info and slices info in two tables.
//
// (1) We store range info in {kv_range_table_name} table using the
// following format:
//
// range_key: [table_name + range_start_key];
// range_record: [range_id + range_version + version +
//                  segment_cnt_of_slices]
//
// (2) We store slices info in {kv_range_slices_table_name} table.
// For each range contains much(about 16384) slices, to avoid a item too
// large, we store the range slices info segmentally.
//
// segment_key: [table_name + range_id + segment_id];
// segment_record: [version + (slice_key+slice_size) +
//                          (slice_key+slice_size) +...];

std::string DataStoreServiceClient::EncodeRangeKey(
    const txservice::TableName &table_name,
    const txservice::TxKey &range_start_key)
{
    std::string key;
    auto table_sv = table_name.StringView();
    key.reserve(table_sv.size() + range_start_key.Size());
    key.append(table_sv);
    if (range_start_key.Type() == txservice::KeyType::NegativeInf)
    {
        const txservice::TxKey *packed_neginf =
            txservice::TxKeyFactory::PackedNegativeInfinity();
        key.append(packed_neginf->Data(), packed_neginf->Size());
    }
    else
    {
        key.append(range_start_key.Data(), range_start_key.Size());
    }

    return key;
}

std::string DataStoreServiceClient::EncodeRangeValue(int32_t range_id,
                                                     uint64_t range_version,
                                                     uint64_t version,
                                                     uint32_t segment_cnt)
{
    std::string kv_range_record;
    kv_range_record.reserve(sizeof(int32_t) + sizeof(uint64_t) +
                            sizeof(uint64_t) + sizeof(uint32_t));
    kv_range_record.append(reinterpret_cast<const char *>(&range_id),
                           sizeof(int32_t));
    kv_range_record.append(reinterpret_cast<const char *>(&range_version),
                           sizeof(uint64_t));
    kv_range_record.append(reinterpret_cast<const char *>(&version),
                           sizeof(uint64_t));
    // segment_cnt of slices
    kv_range_record.append(reinterpret_cast<const char *>(&segment_cnt),
                           sizeof(uint32_t));
    return kv_range_record;
}

std::string DataStoreServiceClient::EncodeRangeSliceKey(
    const txservice::TableName &table_name,
    int32_t range_id,
    uint32_t segment_id)
{
    std::string key;
    auto table_sv = table_name.StringView();
    key.reserve(table_sv.size() + sizeof(range_id) + sizeof(segment_id));
    key.append(table_sv);
    // Due to all read operations of range slices are point reads not scan,
    // we just small endian encoding value of range_id and segment_id instead of
    // big endian encoding.
    key.append(reinterpret_cast<const char *>(&range_id), sizeof(range_id));
    key.append(reinterpret_cast<const char *>(&segment_id), sizeof(segment_id));
    return key;
}

bool DataStoreServiceClient::UpdateRangeSlices(
    const txservice::TableName &table_name,
    uint64_t version,
    txservice::TxKey range_start_key,
    std::vector<const txservice::StoreSlice *> slices,
    int32_t partition_id,
    uint64_t range_version)
{
    // 1- store range_slices info into {kv_range_slices_table_name}
    std::vector<std::string> segment_keys;
    std::vector<std::string> segment_records;
    uint32_t segment_cnt = 0;

    std::string segment_key =
        EncodeRangeSliceKey(table_name, partition_id, segment_cnt);
    std::string segment_record;
    size_t batch_size = segment_key.size() + sizeof(uint64_t);
    segment_record.reserve(MAX_WRITE_BATCH_SIZE - segment_key.size());
    segment_record.append(reinterpret_cast<const char *>(&version),
                          sizeof(uint64_t));
    batch_size += sizeof(uint64_t);

    for (size_t i = 0; i < slices.size(); ++i)
    {
        txservice::TxKey slice_start_key = slices[i]->StartTxKey();
        if (slice_start_key.Type() == txservice::KeyType::NegativeInf)
        {
            slice_start_key = txservice::TxKeyFactory::PackedNegativeInfinity()
                                  ->GetShallowCopy();
        }
        uint32_t key_size = static_cast<uint32_t>(slice_start_key.Size());
        batch_size += sizeof(uint32_t);
        batch_size += key_size;

        if (batch_size >= MAX_WRITE_BATCH_SIZE)
        {
            segment_keys.emplace_back(std::move(segment_key));
            segment_records.emplace_back(std::move(segment_record));

            segment_cnt++;
            segment_key =
                EncodeRangeSliceKey(table_name, partition_id, segment_cnt);
            batch_size = segment_key.size();

            segment_record.clear();
            segment_record.reserve(MAX_WRITE_BATCH_SIZE - segment_key.size());
            segment_record.append(reinterpret_cast<const char *>(&version),
                                  sizeof(uint64_t));
            batch_size += sizeof(uint64_t);
        }

        segment_record.append(reinterpret_cast<const char *>(&key_size),
                              sizeof(uint32_t));
        segment_record.append(slice_start_key.Data(), key_size);
        uint32_t slice_size = static_cast<uint32_t>(slices[i]->Size());
        segment_record.append(reinterpret_cast<const char *>(&slice_size),
                              sizeof(uint32_t));
    }
    if (segment_record.size() > 0)
    {
        segment_keys.emplace_back(std::move(segment_key));
        segment_records.emplace_back(std::move(segment_record));
        segment_cnt++;
    }

    assert(segment_keys.size() == segment_cnt);

    // 2- write the segments to storage
    // Calculate kv_partition_id based on table_name.
    int32_t kv_partition_id = KvPartitionIdOf(table_name);
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;

    for (size_t i = 0; i < segment_keys.size(); ++i)
    {
        keys.emplace_back(segment_keys[i]);
        records.emplace_back(segment_records[i]);
        records_ts.emplace_back(version);
        records_ttl.emplace_back(0);  // no ttl
        op_types.emplace_back(WriteOpType::PUT);

        // For segments are splitted based on MAX_WRITE_BATCH_SIZE, execute
        // one write request for each segment record.
        callback_data.Reset();
        BatchWriteRecords(kv_range_slices_table_name,
                          kv_partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          true,
                          &callback_data,
                          &SyncCallback);
        callback_data.Wait();

        if (callback_data.Result().error_code() !=
            EloqDS::remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING) << "UpdateRangeSlices: Failed to write segments.";
            return false;
        }
    }

    // 3- store range info into {kv_range_table_name}
    keys.clear();
    records.clear();
    records_ts.clear();
    records_ttl.clear();
    op_types.clear();
    callback_data.Reset();

    std::string key_str = EncodeRangeKey(table_name, range_start_key);
    std::string rec_str =
        EncodeRangeValue(partition_id, range_version, version, segment_cnt);

    keys.emplace_back(key_str);
    records.emplace_back(rec_str);

    records_ts.emplace_back(version);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);
    BatchWriteRecords(kv_range_table_name,
                      kv_partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      true,
                      &callback_data,
                      &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpdateRangeSlices: Failed to write range info.";
        return false;
    }

    return true;
}

bool DataStoreServiceClient::UpsertRanges(
    const txservice::TableName &table_name,
    std::vector<txservice::SplitRangeInfo> range_info,
    uint64_t version)
{
    assert(table_name.StringView() != txservice::empty_sv);

    for (auto &range : range_info)
    {
        if (!UpdateRangeSlices(table_name,
                               version,
                               std::move(range.start_key_),
                               std::move(range.slices_),
                               range.partition_id_,
                               version))
        {
            return false;
        }
    }

    SyncCallbackData callback_data;
    std::vector<std::string> kv_range_table_names;
    kv_range_table_names.emplace_back(kv_range_table_name);
    FlushData(kv_range_table_names, &callback_data, &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpsertRanges: Failed to flush ranges. Error: "
                     << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::FetchTable(const txservice::TableName &table_name,
                                        std::string &schema_image,
                                        bool &found,
                                        uint64_t &version_ts)
{
    FetchTableCallbackData callback_data(schema_image, found, version_ts);
    Read(kv_table_catalogs_name,
         0,
         table_name.StringView(),
         &callback_data,
         &FetchTableCallback);
    callback_data.Wait();

    if (callback_data.HasError())
    {
        LOG(WARNING) << "FetchTable error: "
                     << callback_data.Result().error_msg();
    }

    return !callback_data.HasError();
}

bool DataStoreServiceClient::DiscoverAllTableNames(
    std::vector<std::string> &norm_name_vec,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr)
{
    int32_t partition_id = 0;

    DiscoverAllTableNamesCallbackData callback_data(kv_table_catalogs_name,
                                                    norm_name_vec,
                                                    yield_fptr,
                                                    resume_fptr,
                                                    partition_id,
                                                    10);

    ScanNext(callback_data.kv_table_name_,
             callback_data.partition_id_,
             callback_data.start_key_,
             callback_data.end_key_,
             callback_data.session_id_,
             false,
             false,
             true,
             callback_data.batch_size_,
             &callback_data.search_conds_,
             &callback_data,
             &DiscoverAllTableNamesCallback);
    callback_data.Wait();

    return !callback_data.HasError();
}

// The store format of database catalog in kvstore is as follows:
//
// key: dbname
// value: db_definition
bool DataStoreServiceClient::UpsertDatabase(std::string_view db,
                                            std::string_view definition)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;
    uint64_t now =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();

    keys.emplace_back(db);
    records.emplace_back(definition);
    records_ts.emplace_back(now);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);

    BatchWriteRecords(kv_database_catalogs_name,
                      0,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      &callback_data,
                      &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "UpsertDatabase failed, error:"
                   << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::DropDatabase(std::string_view db)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;
    uint64_t now =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();

    keys.emplace_back(db);
    records.emplace_back(std::string_view());
    records_ts.emplace_back(now);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::DELETE);

    BatchWriteRecords(kv_database_catalogs_name,
                      0,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      &callback_data,
                      &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DropDatabase failed, error:"
                   << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::FetchDatabase(
    std::string_view db,
    std::string &definition,
    bool &found,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr)
{
    FetchDatabaseCallbackData callback_data(
        definition, found, yield_fptr, resume_fptr);

    Read(kv_database_catalogs_name,
         0,
         db,
         &callback_data,
         &FetchDatabaseCallback);
    callback_data.Wait();

    return !callback_data.HasError();
}

bool DataStoreServiceClient::FetchAllDatabase(
    std::vector<std::string> &dbnames,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr)
{
    FetchAllDatabaseCallbackData callback_data(
        kv_database_catalogs_name, dbnames, yield_fptr, resume_fptr, 0, 100);

    ScanNext(callback_data.kv_table_name_,
             callback_data.partition_id_,
             callback_data.start_key_,
             callback_data.end_key_,
             callback_data.session_id_,
             false,
             false,
             true,
             callback_data.batch_size_,
             &callback_data.search_conds_,
             &callback_data,
             &FetchAllDatabaseCallback);
    callback_data.Wait();

    return !callback_data.HasError();
}

bool DataStoreServiceClient::DropKvTable(const std::string &kv_table_name)
{
    SyncCallbackData callback_data;
    DropTable(std::string_view(kv_table_name.data(), kv_table_name.size()),
              &callback_data,
              &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "DataStoreHandler: Failed to do DropKvTable.";
        return false;
    }

    return true;
}

// NOTICE: this function is not atomic
void DataStoreServiceClient::DropKvTableAsync(const std::string &kv_table_name)
{
    AsyncDropTableCallbackData *callback_data =
        new AsyncDropTableCallbackData();
    callback_data->kv_table_name_ = kv_table_name;
    DropTable(std::string_view(callback_data->kv_table_name_.data(),
                               callback_data->kv_table_name_.size()),
              callback_data,
              &AsyncDropTableCallback);
}

std::string DataStoreServiceClient::CreateKVCatalogInfo(
    const txservice::TableSchema *table_schema) const
{
    boost::uuids::random_generator generator;

    txservice::KVCatalogInfo kv_info;
    kv_info.kv_table_name_ =
        std::string("t").append(boost::lexical_cast<std::string>(generator()));

    std::vector<txservice::TableName> index_names = table_schema->IndexNames();
    for (auto idx_it = index_names.begin(); idx_it < index_names.end();
         ++idx_it)
    {
        if (idx_it->Type() == txservice::TableType::Secondary)
        {
            kv_info.kv_index_names_.emplace(
                *idx_it,
                std::string("i").append(
                    boost::lexical_cast<std::string>(generator())));
        }
        else
        {
            assert((idx_it->Type() == txservice::TableType::UniqueSecondary));
            kv_info.kv_index_names_.emplace(
                *idx_it,
                std::string("u").append(
                    boost::lexical_cast<std::string>(generator())));
        }
    }
    return kv_info.Serialize();
}

txservice::KVCatalogInfo::uptr DataStoreServiceClient::DeserializeKVCatalogInfo(
    const std::string &kv_info_str, size_t &offset) const
{
    txservice::KVCatalogInfo::uptr kv_info =
        std::make_unique<txservice::KVCatalogInfo>();
    kv_info->Deserialize(kv_info_str.data(), offset);
    return kv_info;
}

std::string DataStoreServiceClient::CreateNewKVCatalogInfo(
    const txservice::TableName &table_name,
    const txservice::TableSchema *current_table_schema,
    txservice::AlterTableInfo &alter_table_info)
{
    // Get current kv catalog info.
    const txservice::KVCatalogInfo *current_kv_catalog_info =
        static_cast<const txservice::KVCatalogInfo *>(
            current_table_schema->GetKVCatalogInfo());

    std::string new_kv_info, kv_table_name, new_kv_index_names;

    /* kv table name using current table name */
    kv_table_name = current_kv_catalog_info->kv_table_name_;
    uint32_t kv_val_len = kv_table_name.length();
    new_kv_info
        .append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
        .append(kv_table_name.data(), kv_val_len);

    /* kv index names using new schema index names */
    // 1. remove dropped index kv name
    bool dropped = false;
    for (auto kv_index_it = current_kv_catalog_info->kv_index_names_.cbegin();
         kv_index_it != current_kv_catalog_info->kv_index_names_.cend();
         ++kv_index_it)
    {
        // Check if the index will be dropped.
        dropped = false;
        for (auto drop_index_it = alter_table_info.index_drop_names_.cbegin();
             alter_table_info.index_drop_count_ > 0 &&
             drop_index_it != alter_table_info.index_drop_names_.cend();
             drop_index_it++)
        {
            if (kv_index_it->first == drop_index_it->first)
            {
                dropped = true;
                // Remove dropped index
                alter_table_info.index_drop_names_[kv_index_it->first] =
                    kv_index_it->second;
                break;
            }
        }
        if (!dropped)
        {
            new_kv_index_names.append(kv_index_it->first.String())
                .append(" ")
                .append(kv_index_it->second)
                .append(" ")
                .append(1, static_cast<char>(kv_index_it->first.Engine()))
                .append(" ");
        }
    }
    assert(alter_table_info.index_drop_names_.size() ==
           alter_table_info.index_drop_count_);

    // 2. add new index
    boost::uuids::random_generator generator;
    for (auto add_index_it = alter_table_info.index_add_names_.cbegin();
         alter_table_info.index_add_count_ > 0 &&
         add_index_it != alter_table_info.index_add_names_.cend();
         add_index_it++)
    {
        // get index kv table name
        std::string add_index_kv_name;
        if (add_index_it->first.Type() == txservice::TableType::Secondary)
        {
            add_index_kv_name = std::string("i").append(
                boost::lexical_cast<std::string>(generator()));
        }
        else
        {
            assert(add_index_it->first.Type() ==
                   txservice::TableType::UniqueSecondary);
            add_index_kv_name = std::string("u").append(
                boost::lexical_cast<std::string>(generator()));
        }

        new_kv_index_names.append(add_index_it->first.String())
            .append(" ")
            .append(add_index_kv_name.data())
            .append(" ")
            .append(1, static_cast<char>(add_index_it->first.Engine()))
            .append(" ");

        // set index kv table name
        alter_table_info.index_add_names_[add_index_it->first] =
            add_index_kv_name;
    }
    assert(alter_table_info.index_add_names_.size() ==
           alter_table_info.index_add_count_);

    /* create final new kv info */
    kv_val_len = new_kv_index_names.size();
    new_kv_info
        .append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
        .append(new_kv_index_names.data(), kv_val_len);

    return new_kv_info;
}

uint32_t DataStoreServiceClient::HashArchiveKey(
    const std::string &kv_table_name, const txservice::TxKey &tx_key)
{
    std::string_view tablename_sv =
        std::string_view(kv_table_name.data(), kv_table_name.size());
    size_t kv_table_name_hash = std::hash<std::string_view>()(tablename_sv);
    std::string_view key_sv = std::string_view(tx_key.Data(), tx_key.Size());
    size_t key_hash = std::hash<std::string_view>()(key_sv);
    uint32_t partition_id =
        (kv_table_name_hash ^ (key_hash << 1)) & 0x3FF;  // 1024 partitions
    return partition_id;
}

std::string DataStoreServiceClient::EncodeArchiveKey(
    std::string_view table_name, std::string_view key, uint64_t be_commit_ts)
{
    std::string archive_key;
    archive_key.reserve(table_name.size() + key.size() + KEY_SEPARATOR.size());
    archive_key.append(table_name);
    archive_key.append(KEY_SEPARATOR);
    archive_key.append(key);
    archive_key.append(KEY_SEPARATOR);
    archive_key.append(reinterpret_cast<const char *>(&be_commit_ts),
                       sizeof(uint64_t));
    return archive_key;
}

void DataStoreServiceClient::EncodeArchiveKey(
    std::string_view table_name,
    std::string_view key,
    uint64_t &be_commit_ts,
    std::vector<std::string_view> &keys,
    uint64_t &write_batch_size)
{
    keys.emplace_back(table_name);
    write_batch_size += table_name.size();

    keys.emplace_back(KEY_SEPARATOR);
    write_batch_size += KEY_SEPARATOR.size();

    keys.emplace_back(key);
    write_batch_size += key.size();

    keys.emplace_back(KEY_SEPARATOR);
    write_batch_size += KEY_SEPARATOR.size();

    keys.emplace_back(reinterpret_cast<const char *>(&be_commit_ts),
                      sizeof(uint64_t));
    write_batch_size += sizeof(uint64_t);
}

bool DataStoreServiceClient::DecodeArchiveKey(const std::string &archive_key,
                                              std::string &table_name,
                                              txservice::TxKey &key,
                                              uint64_t &be_commit_ts)
{
    // Find the first separator
    size_t first_sep = archive_key.find(KEY_SEPARATOR);
    if (first_sep == std::string::npos)
    {
        return false;
    }

    // Extract table_name
    table_name = archive_key.substr(0, first_sep);

    // Find the second separator
    size_t second_sep =
        archive_key.find(KEY_SEPARATOR, first_sep + KEY_SEPARATOR.size());
    if (second_sep == std::string::npos)
    {
        return false;
    }

    // Extract key
    size_t key_start = first_sep + KEY_SEPARATOR.size();
    size_t key_length = second_sep - key_start;
    key = txservice::TxKeyFactory::CreateTxKey(archive_key.data() + key_start,
                                               key_length);

    // Extract commit_ts
    size_t ts_pos = second_sep + KEY_SEPARATOR.size();
    if (ts_pos + sizeof(uint64_t) > archive_key.size())
    {
        return false;
    }
    be_commit_ts =
        *reinterpret_cast<const uint64_t *>(archive_key.data() + ts_pos);

    return true;
}

void DataStoreServiceClient::EncodeArchiveValue(
    bool is_deleted,
    const txservice::TxRecord *value,
    size_t &unpack_info_size,
    size_t &encoded_blob_size,
    std::vector<std::string_view> &record_parts,
    size_t &write_batch_size)
{
    static const bool deleted = true;
    static const bool not_deleted = false;
    if (is_deleted)
    {
        record_parts.emplace_back(reinterpret_cast<const char *>(&deleted),
                                  sizeof(bool));
        write_batch_size += sizeof(bool);

        record_parts.emplace_back(std::string_view());  // unpack_info_size
        record_parts.emplace_back(std::string_view());  // unpack_info_data
        record_parts.emplace_back(std::string_view());  // encoded_blob_size
        record_parts.emplace_back(std::string_view());  // encoded_blob_data
    }
    else
    {
        record_parts.emplace_back(reinterpret_cast<const char *>(&not_deleted),
                                  sizeof(bool));
        write_batch_size += sizeof(bool);

        // Here copy the similar logic as EloqRecord Serialize function
        // for best of performance.
        record_parts.emplace_back(
            std::string_view(reinterpret_cast<const char *>(&unpack_info_size),
                             sizeof(uint64_t)));
        write_batch_size += sizeof(uint64_t);

        record_parts.emplace_back(value->UnpackInfoData(),
                                  value->UnpackInfoSize());
        write_batch_size += value->UnpackInfoSize();

        record_parts.emplace_back(
            std::string_view(reinterpret_cast<const char *>(&encoded_blob_size),
                             sizeof(uint64_t)));
        write_batch_size += sizeof(uint64_t);

        record_parts.emplace_back(value->EncodedBlobData(),
                                  value->EncodedBlobSize());
        write_batch_size += value->EncodedBlobSize();
    }
}

void DataStoreServiceClient::DecodeArchiveValue(
    const std::string &archive_value, bool &is_deleted, size_t &value_offset)
{
    size_t pos = 0;
    is_deleted = *reinterpret_cast<const bool *>(archive_value.data() + pos);
    pos += sizeof(bool);
    value_offset = pos;
}

bool DataStoreServiceClient::PutArchivesAll(
    uint32_t node_group,
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    std::vector<txservice::FlushRecord> &batch)
{
    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);

    std::vector<std::string_view> keys;
    // temporary storage for the records in between batch
    // for keeping record upack info and encoded blob sizes
    std::vector<uint64_t> record_tmp_mem_area;
    record_tmp_mem_area.resize(batch.size() * 2);  // unpack_info_size +
                                                   // encoded_blob_size
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    size_t write_batch_size = 0;
    uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();
    const uint64_t archive_ttl =
        now + 1000 * 60 * 60 * 24;  // default ttl is 1 day for archive record

    uint16_t parts_cnt_per_key = 5;
    uint16_t parts_cnt_per_record = 5;

    // calculate the hash partition of each key in the archive table
    std::unordered_map<int32_t, std::vector<uint32_t>> partitions_map;
    size_t reserve_size = batch.size() / 1024 * 2;
    for (size_t i = 0; i < batch.size(); ++i)
    {
        txservice::TxKey tx_key = batch[i].Key();
        uint32_t partition_id = HashArchiveKey(kv_table_name, tx_key);
        auto [it, inserted] =
            partitions_map.try_emplace(KvPartitionIdOf(partition_id, true));
        if (inserted)
        {
            it->second.reserve(reserve_size);
        }
        it->second.emplace_back(i);
    }

    // Send the batch request
    SyncPutAllData sync_putall;
    uint32_t batch_cnt = 0;
    for (auto part_it = partitions_map.begin(); part_it != partitions_map.end();
         ++part_it)
    {
        auto &flush_recs = part_it->second;
        size_t recs_cnt = flush_recs.size();
        keys.reserve(recs_cnt * parts_cnt_per_key);
        records.reserve(recs_cnt * parts_cnt_per_record);
        records_ts.reserve(recs_cnt);
        records_ttl.reserve(recs_cnt);
        op_types.reserve(recs_cnt);

        for (auto idx : flush_recs)
        {
            // Start a new batch if done with current partition.
            if (write_batch_size >= MAX_WRITE_BATCH_SIZE)
            {
                int32_t part_id = part_it->first;
                BatchWriteRecords(kv_mvcc_archive_name,
                                  part_id,
                                  std::move(keys),
                                  std::move(records),
                                  std::move(records_ts),
                                  std::move(records_ttl),
                                  std::move(op_types),
                                  true,
                                  &sync_putall,
                                  SyncPutAllCallback,
                                  parts_cnt_per_key,
                                  parts_cnt_per_record);
                write_batch_size = 0;
                ++batch_cnt;
            }

            txservice::FlushRecord &ckpt_rec = batch.at(idx);
            txservice::TxKey tx_key = ckpt_rec.Key();

            assert(
                ckpt_rec.payload_status_ == txservice::RecordStatus::Normal ||
                ckpt_rec.payload_status_ == txservice::RecordStatus::Deleted);

            records_ts.push_back(ckpt_rec.commit_ts_);
            write_batch_size += sizeof(uint64_t);  // commit_ts

            records_ttl.push_back(archive_ttl);
            write_batch_size += sizeof(uint64_t);  // ttl

            op_types.push_back(WriteOpType::PUT);
            write_batch_size += sizeof(WriteOpType);

            // Encode key
            // convert commit_ts to big endian
            ckpt_rec.commit_ts_ =
                EloqShare::host_to_big_endian(ckpt_rec.commit_ts_);
            EncodeArchiveKey(kv_table_name,
                             std::string_view(tx_key.Data(), tx_key.Size()),
                             ckpt_rec.commit_ts_,
                             keys,
                             write_batch_size);

            // Encode value
            const txservice::TxRecord *rec = ckpt_rec.Payload();
            std::string record_str;
            if (table_name.Engine() == txservice::TableEngine::EloqKv)
            {
                LOG(ERROR) << "Not Archives for EloqKv";
                assert(false);
            }
            else
            {
                size_t &unpack_info_size = record_tmp_mem_area[idx * 2];
                size_t &encode_blob_size = record_tmp_mem_area[idx * 2 + 1];
                if (rec != nullptr)
                {
                    unpack_info_size = rec->UnpackInfoSize();
                    encode_blob_size = rec->EncodedBlobSize();
                }

                EncodeArchiveValue(ckpt_rec.payload_status_ ==
                                       txservice::RecordStatus::Deleted,
                                   rec,
                                   unpack_info_size,
                                   encode_blob_size,
                                   records,
                                   write_batch_size);
            }
        }

        // Send out the last batch of this partition
        if (keys.size() > 0)
        {
            uint32_t part_id = part_it->first;
            BatchWriteRecords(kv_mvcc_archive_name,
                              part_id,
                              std::move(keys),
                              std::move(records),
                              std::move(records_ts),
                              std::move(records_ttl),
                              std::move(op_types),
                              true,
                              &sync_putall,
                              SyncPutAllCallback,
                              parts_cnt_per_key,
                              parts_cnt_per_record);
            write_batch_size = 0;
            ++batch_cnt;
        }
    }

    // Wait the result.
    {
        std::unique_lock<bthread::Mutex> lk(sync_putall.mux_);
        sync_putall.unfinished_request_cnt_ += batch_cnt;
        sync_putall.all_request_started_ = true;
        while (sync_putall.unfinished_request_cnt_ != 0)
        {
            sync_putall.cv_.wait(lk);
        }
    }

    if (sync_putall.result_.error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "PutArchivesAll failed for error: "
                   << sync_putall.result_.error_msg();
        return false;
    }

    if (txservice::Sharder::Instance().LeaderTerm(node_group) < 0)
    {
        LOG(WARNING) << "DataStoreHandler: leader transferred of ng#"
                     << node_group;
        return false;
    }

    return true;
}

bool DataStoreServiceClient::CopyBaseToArchive(
    std::vector<std::pair<txservice::TxKey, int32_t>> &batch,
    uint32_t node_group,
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema)
{
    constexpr uint32_t MAX_WRITE_BATCH_SIZE = 1000;
    constexpr uint32_t MAX_FLYING_READ_COUNT = 100;

    const std::string &base_kv_table_name =
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);

    std::vector<txservice::FlushRecord> archive_vec;
    archive_vec.reserve(MAX_WRITE_BATCH_SIZE);

    // Prepare the call back datas for a batch
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    size_t flying_cnt = 0;
    int error_code = 0;
    std::vector<ReadBaseForArchiveCallbackData> callback_datas;
    callback_datas.reserve(MAX_WRITE_BATCH_SIZE);
    for (size_t i = 0; i < MAX_WRITE_BATCH_SIZE; ++i)
    {
        callback_datas.emplace_back(mtx, cv, flying_cnt, error_code);
    }

    size_t flush_idx = 0;
    while (flush_idx < batch.size() &&
           txservice::Sharder::Instance().LeaderTerm(node_group) > 0)
    {
        size_t read_cnt = 0;
        for (size_t i = 0; i < MAX_WRITE_BATCH_SIZE && flush_idx < batch.size();
             i++, flush_idx++)
        {
            txservice::TxKey &tx_key = batch[flush_idx].first;
            assert(tx_key.Data() != nullptr && tx_key.Size() > 0);
            uint32_t partition_id = batch[flush_idx].second;
            auto *callback_data = &callback_datas[i];
            callback_data->ResetResult();
            size_t flying_cnt = callback_data->AddFlyingReadCount();
            Read(base_kv_table_name,
                 KvPartitionIdOf(partition_id, true),
                 std::string_view(tx_key.Data(), tx_key.Size()),
                 callback_data,
                 &SyncBatchReadForArchiveCallback);
            read_cnt++;
            if (flying_cnt >= MAX_FLYING_READ_COUNT)
            {
                callback_data->Wait();
            }
            if (callback_data->GetErrorCode() != 0)
            {
                LOG(ERROR) << "CopyBaseToArchive failed for read base table.";
                return false;
            }
        }

        // Wait the result all return.
        std::unique_lock<bthread::Mutex> lk(mtx);
        while (flying_cnt > 0)
        {
            cv.wait(lk);
        }
        lk.unlock();

        // Process the results
        for (size_t i = 0; i < read_cnt; i++)
        {
            auto &callback_data = callback_datas[i];
            txservice::TxKey tx_key = txservice::TxKeyFactory::CreateTxKey(
                callback_data.key_str_.data(), callback_data.key_str_.size());
            std::string_view val = callback_data.value_str_;
            size_t offset = 0;
            bool is_deleted = false;
            std::unique_ptr<txservice::TxRecord> record =
                txservice::TxRecordFactory::CreateTxRecord();
            if (table_name.Engine() == txservice::TableEngine::EloqKv)
            {
                // mvcc is not used for EloqKV
                assert(false);
                txservice::TxObject *tx_object =
                    static_cast<txservice::TxObject *>(record.get());
                record = tx_object->DeserializeObject(val.data(), offset);
            }
            else
            {
                DeserializeTxRecordStr(val, is_deleted, offset);
                if (!is_deleted)
                {
                    record->Deserialize(val.data(), offset);
                }
            }

            auto &ref = archive_vec.emplace_back();
            ref.SetKey(std::move(tx_key));
            ref.commit_ts_ = callback_data.ts_;
            ref.partition_id_ = callback_data.partition_id_;

            if (!is_deleted)
            {
                if (table_name.Engine() == txservice::TableEngine::EloqKv)
                {
                    // should not be here
                    assert(false);
                    ref.SetNonVersionedPayload(record.get());
                }
                else
                {
                    assert(table_name == txservice::Sequences::table_name_ ||
                           table_name.Engine() != txservice::TableEngine::None);
                    ref.SetVersionedPayload(std::move(record));
                }

                ref.payload_status_ = txservice::RecordStatus::Normal;
            }
            else
            {
                ref.payload_status_ = txservice::RecordStatus::Deleted;
            }
        }

        // Put the archive records to the archive table.
        // This is a sync call
        bool ret = PutArchivesAll(node_group,
                                  table_name,
                                  table_schema->GetKVCatalogInfo(),
                                  archive_vec);
        archive_vec.clear();
        if (!ret)
        {
            return false;
        }
    }

    return true;
}

bool DataStoreServiceClient::FetchArchives(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    const txservice::TxKey &key,
    std::vector<txservice::VersionTxRecord> &archives,
    uint64_t from_ts)
{
    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    uint64_t be_from_ts = EloqShare::host_to_big_endian(from_ts);
    std::string lower_bound_key = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), be_from_ts);
    std::string upper_bound_key = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), UINT64_MAX);
    uint32_t partition_id = HashArchiveKey(kv_table_name, key);
    int32_t kv_partition_id = KvPartitionIdOf(partition_id, true);
    size_t batch_size = 100;
    FetchArchivesCallbackData callback_data(kv_mvcc_archive_name,
                                            kv_partition_id,
                                            lower_bound_key,
                                            upper_bound_key,
                                            batch_size,
                                            UINT64_MAX,
                                            true);

    ScanNext(kv_mvcc_archive_name,
             kv_partition_id,
             lower_bound_key,
             upper_bound_key,
             callback_data.session_id_,
             true,                         // include start key
             false,                        // include end key
             callback_data.scan_forward_,  // scan forward: true
             batch_size,
             nullptr,  // search_condition
             &callback_data,
             &FetchArchivesCallback);
    callback_data.Wait();

    if (callback_data.HasError())
    {
        LOG(ERROR) << "FetchVisibleArchive failed, error:"
                   << callback_data.Result().error_msg()
                   << " table_name: " << table_name.StringView()
                   << " key: " << std::string_view(key.Data(), key.Size());
        return false;
    }

    for (size_t i = 0; i < callback_data.archive_values_.size(); ++i)
    {
        const std::string &archive_value_str = callback_data.archive_values_[i];

        bool is_deleted = false;
        std::string value_str;
        size_t value_offset = 0;
        DecodeArchiveValue(archive_value_str, is_deleted, value_offset);

        auto &ref = archives.emplace_back();
        ref.commit_ts_ = callback_data.archive_commit_ts_[i];
        ref.record_status_ = is_deleted ? txservice::RecordStatus::Deleted
                                        : txservice::RecordStatus::Normal;

        if (!is_deleted)
        {
            if (table_name.Engine() == txservice::TableEngine::EloqKv)
            {
                // should not be here
                assert(false);
            }
            else
            {
                std::unique_ptr<txservice::TxRecord> tmp_rec =
                    txservice::TxRecordFactory::CreateTxRecord();
                tmp_rec->Deserialize(archive_value_str.data(), value_offset);
                ref.record_ = std::move(tmp_rec);
            }
        }
    }

    return true;
}

bool DataStoreServiceClient::FetchVisibleArchive(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    const txservice::TxKey &key,
    const uint64_t upper_bound_ts,
    txservice::TxRecord &rec,
    txservice::RecordStatus &rec_status,
    uint64_t &commit_ts)
{
    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    uint64_t be_upper_bound_ts = EloqShare::host_to_big_endian(upper_bound_ts);
    std::string lower_bound_key =
        EncodeArchiveKey(kv_table_name,
                         std::string_view(key.Data(), key.Size()),
                         be_upper_bound_ts);
    std::string upper_bound_key = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), 0);
    uint32_t partition_id = HashArchiveKey(kv_table_name, key);
    int32_t kv_partition_id = KvPartitionIdOf(partition_id, true);
    size_t batch_size = 1;
    FetchArchivesCallbackData callback_data(kv_mvcc_archive_name,
                                            kv_partition_id,
                                            lower_bound_key,
                                            upper_bound_key,
                                            batch_size,
                                            1,  // limit 1
                                            false);
    ScanNext(kv_mvcc_archive_name,
             kv_partition_id,
             lower_bound_key,
             upper_bound_key,
             callback_data.session_id_,
             true,                         // include start key
             false,                        // include end key
             callback_data.scan_forward_,  // scan forward: false
             batch_size,
             nullptr,  // search condition
             &callback_data,
             &FetchArchivesCallback);
    callback_data.Wait();

    if (callback_data.HasError())
    {
        LOG(ERROR) << "FetchVisibleArchive failed, error:"
                   << callback_data.Result().error_msg()
                   << " table_name: " << table_name.StringView()
                   << " key: " << std::string_view(key.Data(), key.Size());
        return false;
    }

    assert(callback_data.archive_values_.size() == 1);
    const std::string &archive_value_str = callback_data.archive_values_[0];

    bool is_deleted = false;
    size_t value_offset = 0;
    DecodeArchiveValue(archive_value_str, is_deleted, value_offset);
    commit_ts = callback_data.archive_commit_ts_[0];

    rec_status = is_deleted ? txservice::RecordStatus::Deleted
                            : txservice::RecordStatus::Normal;
    if (!is_deleted)
    {
        if (table_name.Engine() == txservice::TableEngine::EloqKv)
        {
            // should not be here
            assert(false);
        }
        else
        {
            rec.Deserialize(archive_value_str.data(), value_offset);
        }
    }

    return true;
}

bool DataStoreServiceClient::NeedCopyRange() const
{
    return true;
}

void DataStoreServiceClient::RestoreTxCache(txservice::NodeGroupId cc_ng_id,
                                            int64_t cc_ng_term)
{
    LOG(ERROR) << "RestoreTxCache not implemented";
    assert(false);
}

bool DataStoreServiceClient::OnLeaderStart(uint32_t *next_leader_node)
{
    return true;
}

void DataStoreServiceClient::OnStartFollowing()
{
}

void DataStoreServiceClient::OnShutdown()
{
}

bool DataStoreServiceClient::IsLocalShard(uint32_t shard_id)
{
    // this is a temporary solution for scale up scenario (from one smaller
    // node to another bigger node)
    return cluster_manager_.IsOwnerOfShard(shard_id);
}

bool DataStoreServiceClient::IsLocalPartition(int32_t partition_id)
{
    return cluster_manager_.IsOwnerOfPartition(partition_id);
}

txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::FetchRecord(txservice::FetchRecordCc *fetch_cc)
{
    if (metrics::enable_kv_metrics)
    {
        fetch_cc->start_ = metrics::Clock::now();
    }

    const std::string &kv_table_name = fetch_cc->kv_table_name_;

    int32_t partition_id;
    if (fetch_cc->table_name_.Engine() != txservice::TableEngine::EloqKv)
    {
        partition_id = fetch_cc->range_id_;
    }
    else
    {
        partition_id = MapKeyHashToPartitionId(fetch_cc->tx_key_);
    }

    if (!fetch_cc->tx_key_.IsOwner())
    {
        fetch_cc->tx_key_ = fetch_cc->tx_key_.Clone();
    }

    Read(kv_table_name,
         KvPartitionIdOf(partition_id, false),
         std::string_view(fetch_cc->tx_key_.Data(), fetch_cc->tx_key_.Size()),
         fetch_cc,
         &FetchRecordCallback);

    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

void DataStoreServiceClient::Read(const std::string_view kv_table_name,
                                  const uint32_t partition_id,
                                  const std::string_view key,
                                  void *callback_data,
                                  DataStoreCallback callback)
{
    ReadClosure *read_clouse = read_closure_pool_.NextObject();
    read_clouse->Reset(
        this, kv_table_name, partition_id, key, callback_data, callback);
    ReadInternal(read_clouse);
}

void DataStoreServiceClient::ReadInternal(ReadClosure *read_clouse)
{
    if (IsLocalPartition(read_clouse->PartitionId()))
    {
        read_clouse->PrepareRequest(true);
        data_store_service_->Read(read_clouse->TableName(),
                                  read_clouse->PartitionId(),
                                  read_clouse->Key(),
                                  &read_clouse->LocalValueRef(),
                                  &read_clouse->LocalTsRef(),
                                  &read_clouse->LocalTtlRef(),
                                  &read_clouse->LocalResultRef(),
                                  read_clouse);
    }
    else
    {
        read_clouse->PrepareRequest(false);
        auto channel =
            GetDataStoreServiceChannelByPartitionId(read_clouse->PartitionId());
        if (!channel)
        {
            brpc::ClosureGuard guard(read_clouse);
            ::EloqDS::remote::CommonResult &result = read_clouse->Result();
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::NETWORK_ERROR);
            return;
        }
        read_clouse->SetChannel(channel);

        EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        brpc::Controller &cntl = *read_clouse->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = read_clouse->ReadRequest();
        auto *resp = read_clouse->ReadResponse();
        stub.Read(&cntl, req, resp, read_clouse);
    }
}

void DataStoreServiceClient::DeleteRange(const std::string_view table_name,
                                         const int32_t partition_id,
                                         const std::string &start_key,
                                         const std::string &end_key,
                                         const bool skip_wal,
                                         void *callback_data,
                                         DataStoreCallback callback)
{
    DeleteRangeClosure *closure = delete_range_closure_pool_.NextObject();

    closure->Reset(*this,
                   table_name,
                   partition_id,
                   start_key,
                   end_key,
                   skip_wal,
                   callback_data,
                   callback);

    DeleteRangeInternal(closure);
}

void DataStoreServiceClient::DeleteRangeInternal(
    DeleteRangeClosure *delete_range_clouse)
{
    if (IsLocalPartition(delete_range_clouse->PartitionId()))
    {
        delete_range_clouse->PrepareRequest(true);
        data_store_service_->DeleteRange(delete_range_clouse->TableName(),
                                         delete_range_clouse->PartitionId(),
                                         delete_range_clouse->StartKey(),
                                         delete_range_clouse->EndKey(),
                                         delete_range_clouse->SkipWal(),
                                         delete_range_clouse->Result(),
                                         delete_range_clouse);
    }
    else
    {
        delete_range_clouse->PrepareRequest(false);
        auto channel = GetDataStoreServiceChannelByPartitionId(
            delete_range_clouse->PartitionId());
        if (!channel)
        {
            brpc::ClosureGuard guard(delete_range_clouse);
            ::EloqDS::remote::CommonResult &result =
                delete_range_clouse->Result();
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::NETWORK_ERROR);
            return;
        }
        delete_range_clouse->SetChannel(channel);

        EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        brpc::Controller &cntl = *delete_range_clouse->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = delete_range_clouse->DeleteRangeRequest();
        auto *resp = delete_range_clouse->DeleteRangeResponse();
        stub.DeleteRange(&cntl, req, resp, delete_range_clouse);
    }
}

void DataStoreServiceClient::FlushData(
    const std::vector<std::string> &kv_table_names,
    void *callback_data,
    DataStoreCallback callback)
{
    FlushDataClosure *closure = flush_data_closure_pool_.NextObject();
    auto shards = cluster_manager_.GetAllShards();
    std::vector<uint32_t> shard_ids;
    shard_ids.reserve(shards.size());
    for (auto &[s_id, _] : shards)
    {
        shard_ids.push_back(s_id);
    }

    closure->Reset(
        *this, &kv_table_names, std::move(shard_ids), callback_data, callback);

    FlushDataInternal(closure);
}

void DataStoreServiceClient::FlushDataInternal(
    FlushDataClosure *flush_data_closure)
{
    assert(!flush_data_closure->UnfinishedShards().empty());
    uint32_t shard_id = flush_data_closure->UnfinishedShards().back();
    if (IsLocalShard(shard_id))
    {
        flush_data_closure->PrepareRequest(true);
        data_store_service_->FlushData(flush_data_closure->KvTableNames(),
                                       shard_id,
                                       flush_data_closure->Result(),
                                       flush_data_closure);
    }
    else
    {
        flush_data_closure->PrepareRequest(false);
        auto channel = GetDataStoreServiceChannelByShardId(shard_id);
        if (!channel)
        {
            brpc::ClosureGuard guard(flush_data_closure);
            ::EloqDS::remote::CommonResult &result =
                flush_data_closure->Result();
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::NETWORK_ERROR);
            return;
        }
        flush_data_closure->SetChannel(channel);

        EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        brpc::Controller &cntl = *flush_data_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = flush_data_closure->FlushDataRequest();
        auto *resp = flush_data_closure->FlushDataResponse();
        stub.FlushData(&cntl, req, resp, flush_data_closure);
    }
}

// NOTICE: the DropTable function is not atomic.
void DataStoreServiceClient::DropTable(std::string_view table_name,
                                       void *callback_data,
                                       DataStoreCallback callback)
{
    DLOG(INFO) << "DropTableWithRetry for table: " << table_name;

    DropTableClosure *closure = drop_table_closure_pool_.NextObject();
    auto shards = cluster_manager_.GetAllShards();
    std::vector<uint32_t> shard_ids;
    shard_ids.reserve(shards.size());
    for (auto &[s_id, _] : shards)
    {
        shard_ids.push_back(s_id);
    }

    closure->Reset(
        *this, table_name, std::move(shard_ids), callback_data, callback);

    DropTableInternal(closure);
}

void DataStoreServiceClient::DropTableInternal(
    DropTableClosure *drop_table_closure)
{
    // TODO(lzx): drop table data on all data shards in parallel.
    uint32_t shard_id = drop_table_closure->UnfinishedShards().back();
    if (IsLocalShard(shard_id))
    {
        drop_table_closure->PrepareRequest(true);
        data_store_service_->DropTable(drop_table_closure->TableName(),
                                       shard_id,
                                       drop_table_closure->Result(),
                                       drop_table_closure);
    }
    else
    {
        drop_table_closure->PrepareRequest(false);
        auto channel = GetDataStoreServiceChannelByShardId(shard_id);
        if (!channel)
        {
            brpc::ClosureGuard guard(drop_table_closure);
            ::EloqDS::remote::CommonResult &result =
                drop_table_closure->Result();
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::NETWORK_ERROR);
            return;
        }
        drop_table_closure->SetChannel(channel);

        EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        brpc::Controller &cntl = *drop_table_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = drop_table_closure->DropTableRequest();
        auto *resp = drop_table_closure->DropTableResponse();
        stub.DropTable(&cntl, req, resp, drop_table_closure);
    }
}

void DataStoreServiceClient::ScanNext(
    const std::string_view table_name,
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
    DataStoreCallback callback)
{
    ScanNextClosure *closure = scan_next_closure_pool_.NextObject();
    closure->Reset(*this,
                   table_name,
                   partition_id,
                   start_key,
                   end_key,
                   inclusive_start,
                   inclusive_end,
                   scan_forward,
                   session_id,
                   batch_size,
                   search_conditions,
                   callback_data,
                   callback);
    ScanNextInternal(closure);
}

void DataStoreServiceClient::ScanNextInternal(
    ScanNextClosure *scan_next_closure)
{
    if (IsLocalPartition(scan_next_closure->PartitionId()))
    {
        scan_next_closure->PrepareRequest(true);
        data_store_service_->ScanNext(
            scan_next_closure->TableName(),
            scan_next_closure->PartitionId(),
            scan_next_closure->StartKey(),
            scan_next_closure->EndKey(),
            scan_next_closure->InclusiveStart(),
            scan_next_closure->InclusiveEnd(),
            scan_next_closure->ScanForward(),
            scan_next_closure->BatchSize(),
            scan_next_closure->LocalSearchConditionsPtr(),
            &scan_next_closure->LocalItemsRef(),
            &scan_next_closure->LocalSessionIdRef(),
            &scan_next_closure->Result(),
            scan_next_closure);
    }
    else
    {
        scan_next_closure->PrepareRequest(false);
        auto channel = GetDataStoreServiceChannelByPartitionId(
            scan_next_closure->PartitionId());
        if (!channel)
        {
            brpc::ClosureGuard guard(scan_next_closure);
            ::EloqDS::remote::CommonResult &result =
                scan_next_closure->Result();
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::NETWORK_ERROR);
            return;
        }
        scan_next_closure->SetChannel(channel);

        EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        brpc::Controller &cntl = *scan_next_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = scan_next_closure->ScanNextRequest();
        auto *resp = scan_next_closure->ScanNextResponse();
        stub.ScanNext(&cntl, req, resp, scan_next_closure);
    }
}

void DataStoreServiceClient::ScanClose(const std::string_view table_name,
                                       uint32_t partition_id,
                                       std::string &session_id,
                                       void *callback_data,
                                       DataStoreCallback callback)
{
    ScanNextClosure *closure = scan_next_closure_pool_.NextObject();
    closure->Reset(*this,
                   table_name,
                   partition_id,
                   "",     // start_key (empty for scan close)
                   "",     // end_key (empty for scan close)
                   false,  // inclusive_start
                   false,  // inclusive_end
                   true,   // scan_forward
                   session_id,
                   0,  // batch_size 0 for close
                   nullptr,
                   callback_data,
                   callback);
    ScanCloseInternal(closure);
}

void DataStoreServiceClient::ScanCloseInternal(
    ScanNextClosure *scan_next_closure)
{
    if (IsLocalPartition(scan_next_closure->PartitionId()))
    {
        scan_next_closure->PrepareRequest(true);
        data_store_service_->ScanClose(scan_next_closure->TableName(),
                                       scan_next_closure->PartitionId(),
                                       &scan_next_closure->LocalSessionIdRef(),
                                       &scan_next_closure->LocalResultRef(),
                                       scan_next_closure);
    }
    else
    {
        scan_next_closure->PrepareRequest(false);
        auto channel = GetDataStoreServiceChannelByPartitionId(
            scan_next_closure->PartitionId());
        if (!channel)
        {
            brpc::ClosureGuard guard(scan_next_closure);
            ::EloqDS::remote::CommonResult &result =
                scan_next_closure->Result();
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::NETWORK_ERROR);
            return;
        }
        scan_next_closure->SetChannel(channel);

        EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        brpc::Controller &cntl = *scan_next_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = scan_next_closure->ScanNextRequest();
        auto *resp = scan_next_closure->ScanNextResponse();
        stub.ScanClose(&cntl, req, resp, scan_next_closure);
    }
}

bool DataStoreServiceClient::InitTableRanges(
    const txservice::TableName &table_name, uint64_t version)
{
    // init_partition_id and kv_partition_id
    int32_t kv_partition_id = KvPartitionIdOf(table_name);
    int32_t init_range_id = InitialRangePartitionIdOf(table_name);

    const txservice::TxKey *neg_inf_key =
        txservice::TxKeyFactory::PackedNegativeInfinity();

    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;

    std::string key_str = EncodeRangeKey(table_name, *neg_inf_key);
    std::string rec_str = EncodeRangeValue(init_range_id, version, version, 0);

    keys.emplace_back(std::string_view(key_str.data(), key_str.size()));
    records.emplace_back(std::string_view(rec_str.data(), rec_str.size()));
    records_ts.emplace_back(version);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);
    BatchWriteRecords(kv_range_table_name,
                      kv_partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      &callback_data,
                      &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "InitTableRanges: Failed to write range info.";
        return false;
    }

    return true;
}

bool DataStoreServiceClient::DeleteTableRanges(
    const txservice::TableName &table_name)
{
    int32_t kv_partition_id = KvPartitionIdOf(table_name);
    // delete all slices info from {kv_range_slices_table_name} table
    std::string start_key = table_name.String();
    std::string end_key = start_key;
    end_key.back()++;

    SyncCallbackData callback_data;
    DeleteRange(kv_range_slices_table_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                &callback_data,
                &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableRanges failed, error: "
                   << callback_data.Result().error_msg();
        return false;
    }

    // delete all range info from {kv_range_table_name} table
    callback_data.Reset();
    DeleteRange(kv_range_table_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                &callback_data,
                &SyncCallback);
    callback_data.Wait();
    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableRanges failed, error: "
                   << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::InitTableLastRangePartitionId(
    const txservice::TableName &table_name)
{
    int32_t init_range_id = InitialRangePartitionIdOf(table_name);

    if (txservice::Sequences::Initialized())
    {
        bool res = txservice::Sequences::InitIdOfTableRangePartition(
            table_name, init_range_id);

        DLOG(INFO) << "UpdateLastRangePartition, table: "
                   << table_name.StringView() << ", res: " << (int) res;
        return res;
    }

    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;

    std::pair<txservice::TxKey, txservice::TxRecord::Uptr> seq_pair =
        txservice::Sequences::GetSequenceKeyAndInitRecord(
            table_name,
            txservice::SequenceType::RangePartitionId,
            init_range_id,
            1,
            1,
            init_range_id + 1);
    // See PutAll(): encode is_delete, encoded_blob_data and unpack_info
#ifdef RANGE_PARTITION_ENABLED
    std::string encoded_tx_record =
        SerializeTxRecord(false, seq_pair.second.get());
#else
    auto encoded_tx_record = std::string_view(
        seq_pair.second->EncodedBlobData(), seq_pair.second->EncodedBlobSize());
#endif
    int32_t kv_partition_id =
        KvPartitionIdOf(txservice::Sequences::table_name_);

    for (int i = 0; i < 3; i++)
    {
        // Write directly into sequence table in kvstore.
        callback_data.Reset();
        keys.emplace_back(
            std::string_view(seq_pair.first.Data(), seq_pair.first.Size()));
        records.emplace_back(std::string_view(encoded_tx_record.data(),
                                              encoded_tx_record.size()));
        records_ts.push_back(100U);
        records_ttl.push_back(0U);
        op_types.push_back(WriteOpType::PUT);

        BatchWriteRecords(txservice::Sequences::kv_table_name_sv_,
                          kv_partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          false,
                          &callback_data,
                          &SyncCallback);
        callback_data.Wait();
        if (callback_data.Result().error_code() ==
            EloqDS::remote::DataStoreError::NO_ERROR)
        {
            DLOG(INFO) << "DataStoreHandler:InitTableLastRangePartitionId "
                          "finished. Table: "
                       << table_name.StringView();
            return true;
        }
        else
        {
            LOG(WARNING) << "DataStoreHandler:InitTableLastRangePartitionId "
                            "failed, retrying. Table: "
                         << table_name.StringView()
                         << " Error: " << callback_data.Result().error_msg();
            bthread_usleep(500000U);
        }
    }
    return false;
}

bool DataStoreServiceClient::DeleteTableLastRangePartitionId(
    const txservice::TableName &table_name)
{
    txservice::Sequences::DeleteSequence(
        table_name, txservice::SequenceType::RangePartitionId);
    return true;
}

bool DataStoreServiceClient::DeleteSequence(
    const txservice::TableName &base_table_name)
{
    txservice::Sequences::DeleteSequence(
        base_table_name, txservice::SequenceType::AutoIncrementColumn, true);
    return true;
}

bool DataStoreServiceClient::DeleteTableStatistics(
    const txservice::TableName &base_table_name)
{
    int32_t kv_partition_id = KvPartitionIdOf(base_table_name);

    // delete all sample keys from {kv_table_statistics_name} table
    std::string start_key = base_table_name.String();
    std::string end_key = start_key;
    end_key.back()++;

    SyncCallbackData callback_data;
    DeleteRange(kv_table_statistics_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                &callback_data,
                &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableStatistics failed, error: "
                   << callback_data.Result().error_msg();
        return false;
    }

    // delete table statistics version from  {kv_table_statistics_version_name}
    callback_data.Reset();
    DeleteRange(kv_table_statistics_version_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                &callback_data,
                &SyncCallback);
    callback_data.Wait();

    if (callback_data.Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableStatistics failed, error: "
                   << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

std::shared_ptr<brpc::Channel>
DataStoreServiceClient::GetDataStoreServiceChannelByPartitionId(
    uint32_t partition_id)
{
    return cluster_manager_.GetDataStoreServiceChannelByPartitionId(
        partition_id);
}

std::shared_ptr<brpc::Channel>
DataStoreServiceClient::UpdateDataStoreServiceChannelByPartitionId(
    uint32_t partition_id)
{
    return cluster_manager_.UpdateDataStoreServiceChannelByPartitionId(
        partition_id);
}

std::shared_ptr<brpc::Channel>
DataStoreServiceClient::GetDataStoreServiceChannel(const DSSNode &node)
{
    return cluster_manager_.GetDataStoreServiceChannel(node);
}

std::shared_ptr<brpc::Channel>
DataStoreServiceClient::GetDataStoreServiceChannelByShardId(uint32_t shard_id)
{
    return cluster_manager_.GetDataStoreServiceChannelByShardId(shard_id);
}

std::shared_ptr<brpc::Channel>
DataStoreServiceClient::UpdateDataStoreServiceChannelByShardId(
    uint32_t shard_id)
{
    return cluster_manager_.UpdateDataStoreServiceChannelByShardId(shard_id);
}

std::shared_ptr<brpc::Channel>
DataStoreServiceClient::UpdateDataStoreServiceChannel(const DSSNode &node)
{
    return cluster_manager_.UpdateDataStoreServiceChannel(node);
}

void DataStoreServiceClient::BatchWriteRecords(
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
    uint16_t parts_cnt_per_key,
    uint16_t parts_cnt_per_record)
{
    assert(key_parts.size() % parts_cnt_per_key == 0);
    assert(record_parts.size() % parts_cnt_per_record == 0);
    BatchWriteRecordsClosure *closure = batch_write_closure_pool_.NextObject();

    closure->Reset(*this,
                   kv_table_name,
                   partition_id,
                   std::move(key_parts),
                   std::move(record_parts),
                   std::move(records_ts),
                   std::move(records_ttl),
                   std::move(op_types),
                   skip_wal,
                   callback_data,
                   callback,
                   parts_cnt_per_key,
                   parts_cnt_per_record);

    BatchWriteRecordsInternal(closure);
}

void DataStoreServiceClient::BatchWriteRecordsInternal(
    BatchWriteRecordsClosure *closure)
{
    assert(closure != nullptr);
    uint32_t req_shard_id = GetShardIdByPartitionId(closure->partition_id_);
    closure->SetReqShardId(req_shard_id);

    if (IsLocalShard(req_shard_id))
    {
        closure->is_local_request_ = true;
        data_store_service_->BatchWriteRecords(closure->kv_table_name_,
                                               closure->partition_id_,
                                               closure->key_parts_,
                                               closure->record_parts_,
                                               closure->record_ts_,
                                               closure->record_ttl_,
                                               closure->op_types_,
                                               closure->skip_wal_,
                                               closure->result_,
                                               closure,
                                               closure->PartsCountPerKey(),
                                               closure->PartsCountPerRecord());
    }
    else
    {
        closure->is_local_request_ = false;

        auto channel =
            cluster_manager_.GetDataStoreServiceChannelByShardId(req_shard_id);
        if (!channel)
        {
            // TODO(lzx): retry..
            assert(false);
            closure->result_.set_error_code(
                remote::DataStoreError::NETWORK_ERROR);
            closure->Run();
            return;
        }

        // prepare request
        closure->PrepareRemoteRequest();
        closure->SetChannel(channel);

        // send request
        remote::DataStoreRpcService_Stub stub(channel.get());
        stub.BatchWriteRecords(closure->Controller(),
                               closure->RemoteRequest(),
                               closure->RemoteResponse(),
                               closure);
    }
}

std::string DataStoreServiceClient::SerializeTxRecord(
    bool is_deleted, const txservice::TxRecord *rec)
{
    std::string record;
    record.append(reinterpret_cast<const char *>(&is_deleted), sizeof(bool));
    if (is_deleted)
    {
        return record;
    }
    rec->Serialize(record);
    return record;
}

void DataStoreServiceClient::SerializeTxRecord(
    bool is_deleted,
    const txservice::TxRecord *rec,
    std::vector<size_t> &record_tmp_mem_area,
    std::vector<std::string_view> &record_parts,
    size_t &write_batch_size)
{
    static const bool deleted = true;
    static const bool not_deleted = false;
    if (is_deleted)
    {
        record_parts.emplace_back(reinterpret_cast<const char *>(&deleted),
                                  sizeof(bool));
        write_batch_size += sizeof(bool);
        record_parts.emplace_back(std::string_view());  // unpack_info_size
        record_parts.emplace_back(std::string_view());  // unpack_info_data
        record_parts.emplace_back(std::string_view());  // encoded_blob_size
        record_parts.emplace_back(std::string_view());  // encoded_blob_data
    }
    else
    {
        record_parts.emplace_back(std::string_view(
            reinterpret_cast<const char *>(&not_deleted), sizeof(bool)));
        write_batch_size += sizeof(bool);
        SerializeTxRecord(
            rec, record_tmp_mem_area, record_parts, write_batch_size);
    }
}

void DataStoreServiceClient::SerializeTxRecord(
    const txservice::TxRecord *rec,
    std::vector<size_t> &record_tmp_mem_area,
    std::vector<std::string_view> &record_parts,
    size_t &write_batch_size)
{
    // Here copy the similar logic as EloqRecord Serialize function
    // for best of performance.
    record_tmp_mem_area.emplace_back(rec->UnpackInfoSize());
    size_t *unpack_info_size = &record_tmp_mem_area.back();
    record_parts.emplace_back(std::string_view(
        reinterpret_cast<const char *>(unpack_info_size), sizeof(size_t)));
    write_batch_size += sizeof(size_t);
    record_parts.emplace_back(rec->UnpackInfoData(), rec->UnpackInfoSize());
    write_batch_size += rec->UnpackInfoSize();
    record_tmp_mem_area.emplace_back(rec->EncodedBlobSize());
    uint64_t *encoded_blob_size = &record_tmp_mem_area.back();
    record_parts.emplace_back(std::string_view(
        reinterpret_cast<const char *>(encoded_blob_size), sizeof(size_t)));
    write_batch_size += sizeof(size_t);
    record_parts.emplace_back(rec->EncodedBlobData(), rec->EncodedBlobSize());
    write_batch_size += rec->EncodedBlobSize();
}

bool DataStoreServiceClient::DeserializeTxRecordStr(
    const std::string_view record, bool &is_deleted, size_t &offset)
{
    if (record.size() < (offset + sizeof(bool)))
    {
        return false;
    }

    is_deleted = *reinterpret_cast<const bool *>(record.data() + offset);
    offset += sizeof(bool);
    return true;
}

bool DataStoreServiceClient::InitPreBuiltTables()
{
    int32_t partition_id = 0;
    uint64_t table_version = 100U;
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;

    // Only need to store table catalog to catalog tables.
    for (const auto &[table_name, kv_table_name] : pre_built_table_names_)
    {
        auto tbl_sv = table_name.StringView();
        // check if the table is initialized
        txservice::TableName tablename(tbl_sv,
                                       txservice::TableType::Primary,
                                       txservice::TableEngine::EloqSql);
        std::string catalog_image;
        bool found = false;
        uint64_t version_ts = 0;
        if (!FetchTable(tablename, catalog_image, found, version_ts))
        {
            LOG(WARNING) << "InitPreBuiltTables failed on fetching table.";
            return false;
        }
        if (found)
        {
            assert(catalog_image.size() > 0);
            // update kv_table_name
            // eloqkv catalog image only store kv_table_name.
            pre_built_table_names_.at(table_name) = catalog_image;
            continue;
        }

#ifdef RANGE_PARTITION_ENABLED
        // init table last range partition id
        bool ok = InitTableRanges(tablename, table_version);
        ok &&InitTableLastRangePartitionId(tablename);
        if (!ok)
        {
            LOG(ERROR) << "InitPreBuiltTables failed on initing table ranges.";
            return false;
        }
#endif

        // write catalog to kvstore
        keys.emplace_back(tbl_sv);
        records.emplace_back(kv_table_name);
        records_ts.emplace_back(table_version);
        records_ttl.emplace_back(0);
        op_types.emplace_back(WriteOpType::PUT);
    }

    if (!keys.empty())
    {
        // write init catalog to kvstore
        SyncCallbackData callback_data;
        BatchWriteRecords(kv_table_catalogs_name,
                          partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          false,
                          &callback_data,
                          &SyncCallback);
        callback_data.Wait();

        if (callback_data.Result().error_code() !=
            remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING) << "InitPreBuiltTables failed" << std::endl;
            return false;
        }
    }

    return true;
}

void DataStoreServiceClient::UpsertTable(UpsertTableData *table_data)
{
    std::unique_ptr<UpsertTableData> data_guard(table_data);

    txservice::OperationType op_type = table_data->op_type_;
    auto *table_schema =
        op_type == txservice::OperationType::DropTable ||
                op_type == txservice::OperationType::TruncateTable
            ? table_data->old_table_schema_
            : table_data->new_table_schema_;

    const txservice::TableName &base_table_name =
        table_schema->GetBaseTableName();
    const txservice::KVCatalogInfo *kv_info = table_schema->GetKVCatalogInfo();
    auto *alter_table_info = table_data->alter_table_info_;

    bool ok = true;
    if (op_type == txservice::OperationType::CreateTable)
    {
        // 1- Create kv tables of base and indexes
        // (skip this step for all table data are stored in one cf.)

#ifdef RANGE_PARTITION_ENABLED
        // 2- Init table ranges
        ok = ok && InitTableRanges(base_table_name, table_schema->Version()) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this, table_schema](
                     const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableRanges(p.first, table_schema->Version()); });

        ok = ok && InitTableLastRangePartitionId(base_table_name) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableLastRangePartitionId(p.first); });
#endif

        // 3- Upsert table catalog

        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::Update)
    {
        // only update catalog info.
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::AddIndex)
    {
        // 1- Create kv table of new index
        // (skip this step for all table data are stored in one cf.)

#ifdef RANGE_PARTITION_ENABLED
        // 2- Init table ranges
        ok = ok &&
             std::all_of(
                 alter_table_info->index_add_names_.begin(),
                 alter_table_info->index_add_names_.end(),
                 [this, table_schema](
                     const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableRanges(p.first, table_schema->Version()); });

        ok = ok &&
             std::all_of(
                 alter_table_info->index_add_names_.begin(),
                 alter_table_info->index_add_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableLastRangePartitionId(p.first); });
#endif
        // 3- Upsert table catalog
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::DropIndex)
    {
        // 1- Drop kv table of indexes
        ok = ok &&
             std::all_of(
                 alter_table_info->index_drop_names_.begin(),
                 alter_table_info->index_drop_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DropKvTable(p.second); });

#ifdef RANGE_PARTITION_ENABLED
        // 2- Delete table ranges of the dropped index
        ok = ok &&
             std::all_of(
                 alter_table_info->index_drop_names_.begin(),
                 alter_table_info->index_drop_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableRanges(p.first); });

        ok = ok &&
             std::all_of(
                 alter_table_info->index_drop_names_.begin(),
                 alter_table_info->index_drop_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableLastRangePartitionId(p.first); });
#endif

        // 3- Upsert table catalog
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::DropTable)
    {
        // 1- Drop kv tables of base and index tables
        ok = ok && DropKvTable(kv_info->kv_table_name_) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DropKvTable(p.second); });

#ifdef RANGE_PARTITION_ENABLED
        // 2- Delete table ranges of  base and index tables

        ok = ok && DeleteTableRanges(base_table_name) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableRanges(p.first); });

        ok = ok && DeleteTableLastRangePartitionId(base_table_name) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableLastRangePartitionId(p.first); });
#endif

        // 3- Delete sequence info of this table
        if (ok)
        {
            const txservice::RecordSchema *rsch = table_schema->RecordSchema();
            if (rsch->AutoIncrementIndex() >= 0)
            {
                // For CREATE TABLE, will write the initial sequence record into
                // the sequence ccmap, and the record will be flush into the
                // data store during normal checkpoint, so there is no need to
                // insert the initial sequence record into data store here.
                ok = DeleteSequence(base_table_name);
            }
        }

        // 4- Delete table statistics
        ok = ok && DeleteTableStatistics(base_table_name);

        // 5- Delete table catalog
        ok = ok && DeleteCatalog(base_table_name, table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::TruncateTable)
    {
        // 1- Drop kv tables of base table
        assert(kv_info->kv_index_names_.empty());
        ok = ok && DropKvTable(kv_info->kv_table_name_);

        // 2- delete sequence info of this table (must execute before
        // InitTableLastRangePartitionId)
        if (ok)
        {
            const txservice::RecordSchema *rsch = table_schema->RecordSchema();
            if (rsch->AutoIncrementIndex() >= 0)
            {
                // For CREATE TABLE, will write the initial sequence record into
                // the sequence ccmap, and the record will be flush into the
                // data store during normal checkpoint, so there is no need to
                // insert the initial sequence record into data store here.
                ok = DeleteSequence(base_table_name);
            }
        }

#ifdef RANGE_PARTITION_ENABLED
        // 3- Reset table ranges of  base and index tables
        ok = ok && DeleteTableRanges(base_table_name) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableRanges(p.first); });

        ok = ok && DeleteTableLastRangePartitionId(base_table_name) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableLastRangePartitionId(p.first); });

        auto *new_table_schema = table_data->new_table_schema_;
        ok = ok &&
             std::all_of(
                 alter_table_info->index_add_names_.begin(),
                 alter_table_info->index_add_names_.end(),
                 [this, new_table_schema](
                     const std::pair<txservice::TableName, std::string> &p) {
                     return InitTableRanges(p.first,
                                            new_table_schema->Version());
                 });

        ok = ok &&
             std::all_of(
                 alter_table_info->index_add_names_.begin(),
                 alter_table_info->index_add_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableLastRangePartitionId(p.first); });
#endif

        // 4- Delete table statistics
        ok = ok && DeleteTableStatistics(base_table_name);

        // 5- update table catalog
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else
    {
        LOG(ERROR) << "UpsertTable: unknown operation type"
                   << " table name: " << base_table_name.StringView();
        assert(false);
    }

    if (ok)
    {
        table_data->SetFinished();
    }
    else
    {
        table_data->SetError(txservice::CcErrorCode::DATA_STORE_ERR);
    }
}

// The store format of table catalog in kvstore is as follows:
//
// key: base_table_name
// value: catalog_image
bool DataStoreServiceClient::UpsertCatalog(
    const txservice::TableSchema *table_schema, uint64_t write_time)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;

    // Save table catalog image
    const txservice::TableName &base_table_name =
        table_schema->GetBaseTableName();
    const std::string &catalog_image = table_schema->SchemaImage();
    int32_t partition_id = 0;

    keys.emplace_back(base_table_name.StringView());
    records.emplace_back(
        std::string_view(catalog_image.data(), catalog_image.size()));
    records_ts.emplace_back(write_time);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);

    BatchWriteRecords(kv_table_catalogs_name,
                      partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      &callback_data,
                      &SyncCallback);

    callback_data.Wait();
    if (callback_data.Result().error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "UpsertCatalog: failed to upsert table catalog, error:"
                   << callback_data.Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::DeleteCatalog(
    const txservice::TableName &base_table_name, uint64_t write_time)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData callback_data;

    // Delete table catalog image
    int32_t partition_id = 0;

    keys.emplace_back(base_table_name.StringView());
    records.emplace_back(std::string_view());
    records_ts.emplace_back(write_time);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::DELETE);

    BatchWriteRecords(kv_table_catalogs_name,
                      partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      &callback_data,
                      &SyncCallback);

    callback_data.Wait();
    if (callback_data.Result().error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteCatalog: failed to upsert table catalog";
        return false;
    }

    return true;
}

}  // namespace EloqDS