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

#include "data_store_service_client_closure.h"

#include <memory>
#include <string>
#include <utility>

#include "partition.h"  // Partition
#include "tx_service/include/cc/cc_request.h"
#include "tx_service/include/cc/local_cc_shards.h"

namespace EloqDS
{
void SyncCallback(void *data,
                  ::google::protobuf::Closure *closure,
                  DataStoreServiceClient &client,
                  const remote::CommonResult &result)
{
    auto *callback_data = static_cast<SyncCallbackData *>(data);
    callback_data->Result().set_error_code(result.error_code());
    callback_data->Result().set_error_msg(result.error_msg());

    callback_data->Notify();
}

void SyncBatchReadForArchiveCallback(void *data,
                                     ::google::protobuf::Closure *closure,
                                     DataStoreServiceClient &client,
                                     const remote::CommonResult &result)
{
    ReadBaseForArchiveCallbackData *callback_data =
        static_cast<ReadBaseForArchiveCallbackData *>(data);
    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);

    auto err_code = result.error_code();
    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        LOG(ERROR) << "BatchReadForArchiveCallback, key not found: "
                   << read_closure->Key();
        // callback_data->SetErrorCode(
        //     static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        // assert(false);
        std::string_view key_str = read_closure->Key();
        uint64_t ts = 1U;
        uint64_t ttl = 0U;
        std::string value_str = client.SerializeTxRecord(true, nullptr);
        callback_data->AddResult(read_closure->PartitionId(),
                                 key_str,
                                 std::move(value_str),
                                 ts,
                                 ttl);
        callback_data->DecreaseFlyingReadCount();
        return;
    }
    else if (err_code != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "BatchReadForArchiveCallback, error_code: " << err_code
                   << ", error_msg: " << result.error_msg();
        callback_data->SetErrorCode(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        callback_data->DecreaseFlyingReadCount();
        return;
    }
    else
    {
        std::string_view key_str = read_closure->Key();
        std::string &value_str = read_closure->ValueStringRef();
        uint64_t ts = read_closure->Ts();
        uint64_t ttl = read_closure->Ttl();

        callback_data->AddResult(read_closure->PartitionId(),
                                 key_str,
                                 std::move(value_str),
                                 ts,
                                 ttl);
        callback_data->DecreaseFlyingReadCount();
    }
}

void FetchRecordCallback(void *data,
                         ::google::protobuf::Closure *closure,
                         DataStoreServiceClient &client,
                         const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);

    auto *fetch_cc = static_cast<txservice::FetchRecordCc *>(data);
    auto err_code = result.error_code();

    {
        using txservice::FaultEntry;
        using txservice::FaultInject;
        CODE_FAULT_INJECTOR("FetchRecordFail", {
            LOG(INFO) << "FaultInject FetchRecordFail";

            txservice::FaultInject::Instance().InjectFault("FetchRecordFail",
                                                           "remove");
            err_code = remote::DataStoreError::NETWORK_ERROR;
        });
    }

    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION,
                                           fetch_cc->start_);
        metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
    }

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
        fetch_cc->rec_ts_ = 1U;
        DLOG(INFO) << "====fetch record===notfound=="
                   << " key: " << read_closure->Key()
                   << " status: " << (int) fetch_cc->rec_status_;

        fetch_cc->SetFinish(0);
    }
    else if (err_code == remote::DataStoreError::NO_ERROR)
    {
        uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();
        uint64_t rec_ttl = read_closure->Ttl();
        std::string_view val = read_closure->Value();

        if (fetch_cc->table_name_.Engine() == txservice::TableEngine::EloqKv)
        {
            // Hash partition
            if (rec_ttl > 0 && rec_ttl < now)
            {
                // expired record
                fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
                fetch_cc->rec_ts_ = 1U;
                DLOG(INFO) << "====fetch record===expired=="
                           << " key: " << read_closure->Key()
                           << " status: " << (int) fetch_cc->rec_status_;
                fetch_cc->SetFinish(0);
                return;
            }

            fetch_cc->rec_status_ = txservice::RecordStatus::Normal;
            fetch_cc->rec_ts_ = read_closure->Ts();
            fetch_cc->rec_str_.assign(val.data(), val.size());
            fetch_cc->SetFinish(0);
        }
        else
        {
            // Range partition
            bool is_deleted = false;
            size_t offset = 0;
            if (!DataStoreServiceClient::DeserializeTxRecordStr(
                    val, is_deleted, offset))
            {
                LOG(ERROR) << "====fetch record===decode error=="
                           << " key: " << read_closure->Key()
                           << " status: " << (int) fetch_cc->rec_status_;
                std::abort();
            }

            if (is_deleted)
            {
                fetch_cc->rec_status_ = txservice::RecordStatus::Deleted;
                fetch_cc->rec_ts_ = 1U;
                DLOG(INFO) << "====fetch record===deleted=="
                           << " key: " << read_closure->Key()
                           << " status: " << (int) fetch_cc->rec_status_;
                fetch_cc->SetFinish(0);
                return;
            }

            fetch_cc->rec_status_ = txservice::RecordStatus::Normal;
            fetch_cc->rec_ts_ = read_closure->Ts();
            fetch_cc->rec_str_.clear();
            fetch_cc->rec_str_.resize(val.size() - offset);
            fetch_cc->rec_str_.assign(val.data() + offset, val.size() - offset);
            fetch_cc->SetFinish(0);
        }
    }
    else
    {
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));

        DLOG(INFO) << "====fetch record===error=="
                   << " key: " << read_closure->Key();
    }
}

void AsyncDropTableCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result)
{
    auto *drop_table_data = static_cast<AsyncDropTableCallbackData *>(data);

    if (result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "Drop table failed: " << result.error_msg();
        // TODO(lzx): register (drop) failed table and retry by an timer thread.
    }

    delete drop_table_data;
}

void FetchTableCatalogCallback(void *data,
                               ::google::protobuf::Closure *closure,
                               DataStoreServiceClient &client,
                               const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);

    auto *fetch_cc = static_cast<txservice::FetchCatalogCc *>(data);
    auto err_code = result.error_code();

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_cc->CatalogImage().clear();
        fetch_cc->SetCommitTs(1);
        fetch_cc->SetFinish(txservice::RecordStatus::Deleted, 0);
    }
    else if (err_code == remote::DataStoreError::NO_ERROR)
    {
        fetch_cc->SetCommitTs(read_closure->Ts());
        std::string &catalog_image = fetch_cc->CatalogImage();

        // TODO(lzx): Unify table schema (de)serialization method for EloqSql
        // and EloqKV. And all data_store_handler share one KvCatalogInfo.
        catalog_image.append(read_closure->ValueString());

        LOG(INFO) << "FetchTableCatalogCallback, key:" << read_closure->Key()
                  << ", catalog_image: " << catalog_image;

        fetch_cc->SetFinish(txservice::RecordStatus::Normal, 0);
    }
    else
    {
        fetch_cc->SetFinish(
            txservice::RecordStatus::Unknown,
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
}

void FetchTableCallback(void *data,
                        ::google::protobuf::Closure *closure,
                        DataStoreServiceClient &client,
                        const remote::CommonResult &result)
{
    auto *fetch_table_data = reinterpret_cast<FetchTableCallbackData *>(data);
    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_table_data->found_ = false;
        fetch_table_data->schema_image_.clear();
        fetch_table_data->version_ts_ = 1;
    }
    else if (result.error_code() == remote::DataStoreError::NO_ERROR)
    {
        fetch_table_data->found_ = true;
        fetch_table_data->version_ts_ = read_closure->Ts();
        std::string &catalog_image = fetch_table_data->schema_image_;

        // TODO(lzx): Unify table schema (de)serialization method for EloqSql
        // and EloqKV. And all data_store_handler share one KvCatalogInfo.
        catalog_image.append(read_closure->ValueString());

        DLOG(INFO) << "FetchTableCallback, key:" << read_closure->Key()
                   << ", catalog_image: " << catalog_image;
    }
    else
    {
        fetch_table_data->found_ = false;
        fetch_table_data->schema_image_.clear();
        fetch_table_data->version_ts_ = 1;
    }

    fetch_table_data->Result().set_error_code(result.error_code());
    fetch_table_data->Result().set_error_msg(result.error_msg());

    fetch_table_data->Notify();
}

void SyncPutAllCallback(void *data,
                        ::google::protobuf::Closure *closure,
                        DataStoreServiceClient &client,
                        const remote::CommonResult &result)
{
    auto *callback_data = reinterpret_cast<SyncPutAllData *>(data);
    callback_data->Finish(result);
}

void FetchDatabaseCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result)
{
    FetchDatabaseCallbackData *fetch_data =
        static_cast<FetchDatabaseCallbackData *>(data);
    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        fetch_data->found_ = false;
    }
    else if (result.error_code() == remote::DataStoreError::NO_ERROR)
    {
        fetch_data->found_ = true;

        fetch_data->db_definition_.append(read_closure->ValueString());

        DLOG(INFO) << "FetchDatabaseCallback, key:" << read_closure->Key()
                   << ", db_definition: " << fetch_data->db_definition_;
    }
    else
    {
        fetch_data->found_ = false;
    }

    fetch_data->Result().set_error_code(result.error_code());
    fetch_data->Result().set_error_msg(result.error_msg());

    fetch_data->Notify();
}

void FetchAllDatabaseCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result)
{
    FetchAllDatabaseCallbackData *fetch_data =
        static_cast<FetchAllDatabaseCallbackData *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchAllDatabaseCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_data->Result().set_error_code(result.error_code());
        fetch_data->Result().set_error_msg(result.error_msg());

        fetch_data->Notify();
        return;
    }
    else
    {
        uint32_t items_size = scan_next_closure->ItemsSize();
        DLOG(INFO) << "FetchAllDatabaseCallback, items_size:" << items_size;
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;
        for (uint32_t i = 0; i < items_size; i++)
        {
            scan_next_closure->GetItem(i, key, value, ts, ttl);
            DLOG(INFO) << "FetchAllDatabaseCallback, db_name:" << key
                       << ", db_definition: " << value;
            fetch_data->dbnames_.emplace_back(std::move(key));
        }

        if (items_size < scan_next_closure->BatchSize())
        {
            // has no more data, notify.
            fetch_data->Result().set_error_code(result.error_code());
            fetch_data->Result().set_error_msg(result.error_msg());

            fetch_data->Notify();
            return;
        }
        else
        {
            DLOG(INFO)
                << "FetchAllDatabaseCallback, has more data, continue to scan.";
            // has more data, continue to scan.
            fetch_data->session_id_ = scan_next_closure->SessionId();
            // fetch_data->start_key_ = fetch_data->dbnames_.back();
            client.ScanNext(fetch_data->kv_table_name_,
                            fetch_data->partition_id_,
                            fetch_data->dbnames_.back(),
                            fetch_data->end_key_,
                            fetch_data->session_id_,
                            false,
                            false,
                            true,
                            fetch_data->batch_size_,
                            &fetch_data->search_conds_,
                            fetch_data,
                            &FetchAllDatabaseCallback);
        }
    }
}

void DiscoverAllTableNamesCallback(void *data,
                                   ::google::protobuf::Closure *closure,
                                   DataStoreServiceClient &client,
                                   const remote::CommonResult &result)
{
    DiscoverAllTableNamesCallbackData *fetch_data =
        static_cast<DiscoverAllTableNamesCallbackData *>(data);

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "DiscoverAllTableNamesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_data->Result().set_error_code(result.error_code());
        fetch_data->Result().set_error_msg(result.error_msg());

        fetch_data->Notify();
        return;
    }
    else
    {
        uint32_t items_size = scan_next_closure->ItemsSize();
        DLOG(INFO) << "DiscoverAllTableNamesCallback, items_size:"
                   << items_size;
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;
        for (uint32_t i = 0; i < items_size; i++)
        {
            scan_next_closure->GetItem(i, key, value, ts, ttl);
            if (key == txservice::Sequences::table_name_sv_)
            {
                DLOG(INFO) << "====skip read sequence table name";
                continue;
            }
            fetch_data->table_names_.emplace_back(std::move(key));
        }

        if (items_size < scan_next_closure->BatchSize())
        {
            // has no more data, notify.
            fetch_data->Result().set_error_code(result.error_code());
            fetch_data->Result().set_error_msg(result.error_msg());

            fetch_data->Notify();
            return;
        }
        else
        {
            DLOG(INFO) << "DiscoverAllTableNamesCallback, has more data, "
                          "continue to scan.";
            // has more data, continue to scan.
            fetch_data->session_id_ = scan_next_closure->SessionId();
            // fetch_data->start_key_ = fetch_data->table_names_.back();
            client.ScanNext(fetch_data->kv_table_name_,
                            fetch_data->partition_id_,
                            // fetch_data->start_key_,
                            fetch_data->table_names_.back(),
                            fetch_data->end_key_,
                            fetch_data->session_id_,
                            false,
                            false,
                            true,
                            fetch_data->batch_size_,
                            &fetch_data->search_conds_,
                            fetch_data,
                            &DiscoverAllTableNamesCallback);
        }
    }
}

void FetchTableRangesCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result)
{
    FetchTableRangesCallbackData *fetch_data =
        static_cast<FetchTableRangesCallbackData *>(data);

    txservice::FetchTableRangesCc *fetch_range_cc = fetch_data->fetch_cc_;

    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchTableRangesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();

        fetch_range_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        delete fetch_data;
    }
    else
    {
        txservice::LocalCcShards *shards =
            txservice::Sharder::Instance().GetLocalCcShards();
        std::unique_lock<std::mutex> heap_lk(shards->table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd =
            mi_override_thread(shards->GetTableRangesHeapThreadId());
        mi_heap_t *prev_heap =
            mi_heap_set_default(shards->GetTableRangesHeap());

        uint32_t items_size = scan_next_closure->ItemsSize();
        DLOG(INFO) << "FetchTableRangesCallback, items_size:" << items_size;
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;

        std::string_view table_name_sv =
            fetch_range_cc->table_name_.StringView();

        std::vector<txservice::InitRangeEntry> range_vec;

        for (uint32_t i = 0; i < items_size; i++)
        {
            scan_next_closure->GetItem(i, key, value, ts, ttl);
            assert(value.size() == (sizeof(int32_t) + sizeof(uint64_t) +
                                    sizeof(uint64_t) + sizeof(uint32_t)));
            const char *buf = value.data();
            int32_t partition_id = *(reinterpret_cast<const int32_t *>(buf));
            buf += sizeof(partition_id);
            uint64_t range_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(range_version);
            uint64_t slice_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(slice_version);

            std::string_view start_key_sv(key.data() + table_name_sv.size(),
                                          key.size() - table_name_sv.size());
            DLOG(INFO) << "FetchTableRangesCallback, table_name:"
                       << table_name_sv << ", start_key:" << start_key_sv
                       << ", partition_id: " << partition_id;

            // If the key is 0x00, it means that the key is NegativeInfinity.
            // (see EloqKey::PackedNegativeInfinity)
            if (start_key_sv.size() > 1 || start_key_sv[0] != 0x00)
            {
                txservice::TxKey start_key =
                    txservice::TxKeyFactory::CreateTxKey(start_key_sv.data(),
                                                         start_key_sv.size());
                range_vec.emplace_back(
                    std::move(start_key), partition_id, range_version);
            }
            else
            {
                range_vec.emplace_back(
                    txservice::TxKeyFactory::NegInfTxKey()->GetShallowCopy(),
                    partition_id,
                    range_version);
            }
        }

        mi_heap_set_default(prev_heap);
        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }
        heap_lk.unlock();

        if (items_size < scan_next_closure->BatchSize())
        {
            // Has no more data, notify.

            // When ddl_skip_kv_ is enabled and the range entry is not
            // physically ready, initializes the original range from negative
            // infinity to positive infinity.
            if (range_vec.empty() && fetch_range_cc->EmptyRanges())
            {
                range_vec.emplace_back(
                    txservice::TxKeyFactory::NegInfTxKey()->GetShallowCopy(),
                    Partition::InitialPartitionId(
                        fetch_range_cc->table_name_.StringView()),
                    1);
            }
            fetch_range_cc->AppendTableRanges(std::move(range_vec));
            fetch_range_cc->SetFinish(0);
            delete fetch_data;
        }
        else
        {
            DLOG(INFO) << "FetchTableRangesCallback, has more data, "
                          "continue to scan.";
            // has more data, continue to scan.
            fetch_data->session_id_ = scan_next_closure->SessionId();
            fetch_data->start_key_.clear();
            fetch_data->start_key_.append(key);
            fetch_range_cc->AppendTableRanges(std::move(range_vec));

            client.ScanNext(fetch_data->kv_table_name_,
                            fetch_data->partition_id_,
                            fetch_data->start_key_,
                            fetch_data->end_key_,
                            fetch_data->session_id_,
                            false,
                            false,
                            true,
                            fetch_data->batch_size_,
                            &fetch_data->search_conds_,
                            fetch_data,
                            &FetchTableRangesCallback);
        }
    }
}

void FetchRangeSlicesCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result)
{
    FetchRangeSlicesCallbackData *fetch_data =
        static_cast<FetchRangeSlicesCallbackData *>(data);

    ReadClosure *read_closure = static_cast<ReadClosure *>(closure);

    std::string_view read_val = read_closure->Value();
    auto *fetch_req = fetch_data->fetch_cc_;
    txservice::NodeGroupId ng_id = fetch_req->cc_ng_id_;

    if (fetch_data->step_ == 1U)
    {
        // step-1: fetched range info.
        if (result.error_code() == remote::DataStoreError::KEY_NOT_FOUND)
        {
            // Failed to read range info.
            assert(false);
            // This should only happen if ddl_skip_kv_ is true.
            fetch_req->slice_info_.emplace_back(
                txservice::TxKey(), 0, txservice::SliceStatus::PartiallyCached);
            fetch_req->SetFinish(txservice::CcErrorCode::NO_ERROR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            delete fetch_data;
            return;
        }
        else if (result.error_code() != remote::DataStoreError::NO_ERROR)
        {
            assert(false);
            // Only read partial range slices info. Caller should retry.
            LOG(ERROR) << "Fetch range slices failed: Partial result, keep "
                          "retring";
            fetch_req->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            delete fetch_data;
            return;
        }
        else
        {
            assert(read_closure->TableName() ==
                   fetch_data->kv_range_table_name_);
            assert(read_val.size() == (sizeof(int32_t) + sizeof(uint64_t) +
                                       sizeof(uint64_t) + sizeof(uint32_t)));
            const char *buf = read_val.data();
            int32_t range_partition_id =
                *(reinterpret_cast<const int32_t *>(buf));
            buf += sizeof(range_partition_id);
            uint64_t range_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(range_version);
            uint64_t slice_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(slice_version);
            uint32_t segment_cnt = *(reinterpret_cast<const uint32_t *>(buf));

            assert(range_version == fetch_req->range_entry_->Version());
            assert(range_partition_id ==
                   fetch_req->range_entry_->GetRangeInfo()->PartitionId());

            if (segment_cnt == 0)
            {
                assert(fetch_req->slice_info_.empty());
                // New Table, only has range info , no slice info.
                fetch_req->slice_info_.emplace_back(
                    txservice::TxKey(),
                    0,
                    txservice::SliceStatus::PartiallyCached);
                fetch_req->SetFinish(txservice::CcErrorCode::NO_ERROR);
                txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
                delete fetch_data;
                return;
            }
            else
            {
                fetch_data->step_ = 2U;
                fetch_data->range_partition_id_ = range_partition_id;
                fetch_req->SetSliceVersion(slice_version);
                fetch_req->SetSegmentCnt(segment_cnt);
                fetch_req->SetCurrentSegmentId(0);

                fetch_data->key_ =
                    client.EncodeRangeSliceKey(fetch_req->table_name_,
                                               range_partition_id,
                                               fetch_req->CurrentSegmentId());
                client.Read(fetch_data->kv_range_slices_table_name_,
                            fetch_data->kv_partition_id_,
                            fetch_data->key_,
                            fetch_data,
                            &FetchRangeSlicesCallback);
            }
        }
    }
    else
    {
        assert(fetch_data->step_ == 2U);
        if (result.error_code() == remote::DataStoreError::KEY_NOT_FOUND)
        {
            assert(false);
            // Only read partial range slices info. Caller should retry.
            LOG(ERROR) << "Fetch range slices failed: Partial result, keep "
                          "retring";
            fetch_req->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            delete fetch_data;
            return;
        }
        else if (result.error_code() != remote::DataStoreError::NO_ERROR)
        {
            // Only read partial range slices info. Caller should retry.
            LOG(ERROR) << "Fetch range slices failed: Partial result, keep "
                          "retring";
            fetch_req->SetFinish(txservice::CcErrorCode::DATA_STORE_ERR);
            txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
            delete fetch_data;
            return;
        }
        else
        {
            uint32_t segment_id = fetch_req->CurrentSegmentId();
            const char *buf = read_val.data();
            size_t offset = 0;
            uint64_t slice_version = *(reinterpret_cast<const uint64_t *>(buf));
            buf += sizeof(slice_version);
            offset += sizeof(slice_version);

            txservice::LocalCcShards *shards =
                txservice::Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> heap_lk(
                shards->table_ranges_heap_mux_);
            bool is_override_thd = mi_is_override_thread();
            mi_threadid_t prev_thd =
                mi_override_thread(shards->GetTableRangesHeapThreadId());
            mi_heap_t *prev_heap =
                mi_heap_set_default(shards->GetTableRangesHeap());

            while (offset < read_val.size())
            {
                uint32_t key_len = *(reinterpret_cast<const uint32_t *>(buf));
                buf += sizeof(uint32_t);

                txservice::TxKey tx_key =
                    txservice::TxKeyFactory::CreateTxKey(buf, key_len);
                buf += key_len;

                uint32_t slice_size =
                    *(reinterpret_cast<const uint32_t *>(buf));
                buf += sizeof(uint32_t);

                fetch_req->slice_info_.emplace_back(
                    std::move(tx_key),
                    slice_size,
                    txservice::SliceStatus::PartiallyCached);

                offset += (sizeof(uint32_t) + key_len + sizeof(uint32_t));
            }

            mi_heap_set_default(prev_heap);
            if (is_override_thd)
            {
                mi_override_thread(prev_thd);
            }
            else
            {
                mi_restore_default_thread_id();
            }
            heap_lk.unlock();

            segment_id++;
            if (segment_id == fetch_req->SegmentCnt())
            {
                // All segments are fetched.
                fetch_req->SetFinish(txservice::CcErrorCode::NO_ERROR);
                txservice::Sharder::Instance().UnpinNodeGroupData(ng_id);
                delete fetch_data;
                return;
            }

            fetch_req->SetCurrentSegmentId(segment_id);
            fetch_data->key_ =
                client.EncodeRangeSliceKey(fetch_req->table_name_,
                                           fetch_data->range_partition_id_,
                                           fetch_req->CurrentSegmentId());
            client.Read(fetch_data->kv_range_slices_table_name_,
                        fetch_data->kv_partition_id_,
                        fetch_data->key_,
                        fetch_data,
                        &FetchRangeSlicesCallback);
        }
    }
}

void FetchCurrentTableStatsCallback(void *data,
                                    ::google::protobuf::Closure *closure,
                                    DataStoreServiceClient &client,
                                    const remote::CommonResult &result)
{
    auto *read_closure = static_cast<ReadClosure *>(closure);
    auto *fetch_cc = static_cast<txservice::FetchTableStatisticsCc *>(data);
    auto err_code = result.error_code();
    if (err_code == remote::DataStoreError::KEY_NOT_FOUND)
    {
        // empty statistics
        fetch_cc->SetFinish(0);
    }
    else if (err_code != remote::DataStoreError::NO_ERROR)
    {
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
    }
    else
    {
        const std::string &val = read_closure->ValueString();
        uint64_t ckpt_version = std::stoull(val);
        fetch_cc->SetCurrentVersion(ckpt_version);
        fetch_cc->StoreHandler()->FetchTableStatistics(fetch_cc->CatalogName(),
                                                       fetch_cc);
    }
}

void FetchTableStatsCallback(void *data,
                             ::google::protobuf::Closure *closure,
                             DataStoreServiceClient &client,
                             const remote::CommonResult &result)
{
    FetchTableStatsCallbackData *fetch_data =
        static_cast<FetchTableStatsCallbackData *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();
    auto *fetch_cc = fetch_data->fetch_cc_;

    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchTableStatsCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();

        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));

        delete fetch_data;
        return;
    }
    else
    {
        std::string key;
        std::string value;
        uint64_t ts;
        uint64_t ttl;
        uint32_t items_size = scan_next_closure->ItemsSize();
        DLOG(INFO) << "FetchTableStatsCallback, items_size:" << items_size;
        fetch_data->session_id_ = scan_next_closure->SessionId();
        assert(items_size <= 1);
        if (items_size == 1)
        {
            scan_next_closure->GetItem(0, key, value, ts, ttl);
            DLOG(INFO) << "FetchTableStatsCallback, key:" << key
                       << ", value: " << value << ", ts: " << ts;

            auto &base_table_name = fetch_cc->CatalogName();
            std::string_view base_table_sv = base_table_name.StringView();
            size_t offset =
                base_table_sv.size() + sizeof(uint64_t) + sizeof(uint32_t);

            std::string indexname_str = key.substr(offset);

            const char *value_buf = value.data();
            size_t value_size = value.size();
            offset = 0;
            uint8_t indextype_val =
                *(reinterpret_cast<const uint8_t *>(value_buf));
            offset += sizeof(uint8_t);
            auto indextype = static_cast<txservice::TableType>(indextype_val);
            txservice::TableName indexname(std::move(indexname_str),
                                           indextype,
                                           fetch_cc->CatalogName().Engine());

            uint64_t records_cnt =
                *(reinterpret_cast<const uint64_t *>(value_buf + offset));
            offset += sizeof(uint64_t);
            if (records_cnt > 0)
            {
                fetch_cc->SetRecords(indexname, records_cnt);
            }
            std::vector<txservice::TxKey> samplekeys;

            while (offset < value_size)
            {
                uint32_t samplekey_len =
                    *(reinterpret_cast<const uint32_t *>(value_buf + offset));
                offset += sizeof(uint32_t);

                txservice::TxKey samplekey =
                    txservice::TxKeyFactory::CreateTxKey(value_buf + offset,
                                                         samplekey_len);
                offset += samplekey_len;
                samplekeys.emplace_back(std::move(samplekey));
            }
            fetch_cc->SamplePoolMergeFrom(indexname, std::move(samplekeys));

            // continue to scan
            fetch_data->start_key_ = std::move(key);
            client.ScanNext(fetch_data->kv_table_name_,
                            fetch_data->partition_id_,
                            fetch_data->start_key_,
                            fetch_data->end_key_,
                            fetch_data->session_id_,
                            false,
                            false,
                            true,
                            1,
                            &fetch_data->search_conditions_,
                            fetch_data,
                            &FetchTableStatsCallback);
        }
        else
        {
            // has no more data, notify.

            fetch_cc->SetFinish(0);
            delete fetch_data;
            return;
        }
    }
}

void LoadRangeSliceCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result)
{
    DLOG(INFO) << "====LoadRangeSliceCallback, closure:" << closure
               << ",err:" << result.error_code();
    assert(data != nullptr);
    LoadRangeSliceCallbackData *callback_data =
        static_cast<LoadRangeSliceCallbackData *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto *fill_store_slice_req = callback_data->fill_store_slice_req_;

    if (result.error_code() != EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DataStoreHandler: Failed to do LoadRangeSlice. "
                   << result.error_msg();
        fill_store_slice_req->SetKvFinish(false);
        // recycle the callback data
        PoolableGuard guard(callback_data);
        return;
    }

    // Process records from this batch
    const txservice::TableName &table_name =
        callback_data->fill_store_slice_req_->TblName();
    uint32_t items_size = scan_next_closure->ItemsSize();

    std::string key_str, value_str;
    uint64_t ts, ttl;
    for (uint32_t i = 0; i < items_size; i++)
    {
        scan_next_closure->GetItem(i, key_str, value_str, ts, ttl);
        txservice::TxKey key = txservice::TxKeyFactory::CreateTxKey(
            key_str.data(), key_str.size());
        std::unique_ptr<txservice::TxRecord> record =
            txservice::TxRecordFactory::CreateTxRecord();
        bool is_deleted = false;
        if (table_name.Engine() == txservice::TableEngine::EloqKv)
        {
            // Hash partition
            txservice::TxObject *tx_object =
                reinterpret_cast<txservice::TxObject *>(record.get());
            size_t offset = 0;
            record = tx_object->DeserializeObject(value_str.data(), offset);
        }
        else
        {
            // Range partition
            size_t offset = 0;
            is_deleted = false;

            if (!DataStoreServiceClient::DeserializeTxRecordStr(
                    value_str, is_deleted, offset))
            {
                LOG(ERROR) << "DataStoreServiceClient::LoadRangeSliceCallback: "
                              "Failed to decode is_deleted. txkey: "
                           << key_str;
                assert(false);
                std::abort();
            }

            if (!is_deleted)
            {
                record->Deserialize(value_str.data(), offset);
            }
        }

        fill_store_slice_req->AddDataItem(
            std::move(key), std::move(record), ts, is_deleted);
    }

    callback_data->last_key_ = key_str;

    callback_data->sesssion_id_ = scan_next_closure->GetSessionId();
    if (scan_next_closure->ItemsSize() == callback_data->batch_size_)
    {
        // has more data, continue to scan.
        client.ScanNext(callback_data->kv_table_name_,
                        callback_data->range_partition_id_,
                        callback_data->last_key_,
                        callback_data->end_key_,
                        callback_data->sesssion_id_,
                        false,  // include start_key
                        false,  // include end_key
                        true,   // scan forward
                        callback_data->batch_size_,
                        nullptr,
                        callback_data,
                        &LoadRangeSliceCallback);
    }
    else
    {
        fill_store_slice_req->SetKvFinish(true);
        // recycle the callback data
        PoolableGuard guard(callback_data);
    }
}

void FetchArchivesCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result)
{
    FetchArchivesCallbackData *fetch_data =
        static_cast<FetchArchivesCallbackData *>(data);
    ScanNextClosure *scan_next_closure =
        static_cast<ScanNextClosure *>(closure);
    auto err_code = result.error_code();

    if (err_code != remote::DataStoreError::NO_ERROR)
    {
        DLOG(INFO) << "FetchArchivesCallback, error_code:" << err_code
                   << ", error_msg: " << result.error_msg();
        fetch_data->Result().set_error_code(result.error_code());
        fetch_data->Result().set_error_msg(result.error_msg());
        fetch_data->Notify();
        return;
    }

    uint32_t items_size = scan_next_closure->ItemsSize();
    DLOG(INFO) << "FetchArchivesCallback, items_size:" << items_size;
    std::string archive_key;
    std::string archive_value;
    std::string record_str;
    uint64_t commit_ts;
    uint64_t ttl;

    for (uint32_t i = 0; i < items_size; i++)
    {
        scan_next_closure->GetItem(
            i, archive_key, archive_value, commit_ts, ttl);
        fetch_data->archive_values_.emplace_back(std::move(archive_value));
        fetch_data->archive_commit_ts_.emplace_back(commit_ts);
    }
    // set the start key of next scan batch
    fetch_data->start_key_ = std::move(archive_key);

    if (items_size < scan_next_closure->BatchSize() ||
        items_size >= fetch_data->limit_)
    {
        // has no more data, notify
        fetch_data->Result().set_error_code(remote::DataStoreError::NO_ERROR);
        fetch_data->Notify();
    }
    else
    {
        client.ScanNext(fetch_data->kv_table_name_,
                        fetch_data->partition_id_,
                        fetch_data->start_key_,
                        fetch_data->end_key_,
                        scan_next_closure->SessionId(),
                        false,
                        false,
                        fetch_data->scan_forward_,
                        fetch_data->batch_size_,
                        nullptr,
                        fetch_data,
                        &FetchArchivesCallback);
    }
}

}  // namespace EloqDS
