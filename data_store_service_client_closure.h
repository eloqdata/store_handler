
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

#include <brpc/channel.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "data_store_service_client.h"
#include "data_store_service_scanner.h"
#include "eloq_data_store_service/object_pool.h"

namespace EloqDS
{
typedef void (*DataStoreCallback)(void *data,
                                  ::google::protobuf::Closure *closure,
                                  DataStoreServiceClient &client,
                                  const remote::CommonResult &result);

struct SyncCallbackData
{
    SyncCallbackData() : mtx_(), cv_(), finished_(false)
    {
    }

    virtual ~SyncCallbackData() = default;

    void Reset()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        finished_ = false;
        result_.Clear();
    }

    virtual void Notify()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        finished_ = true;
        cv_.notify_one();
    }

    virtual void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        while (!finished_)
        {
            cv_.wait(lk);
        }
    }

    remote::CommonResult &Result()
    {
        return result_;
    }

    bool HasError()
    {
        return result_.error_code() != remote::DataStoreError::NO_ERROR &&
               result_.error_code() != remote::DataStoreError::KEY_NOT_FOUND;
    }

private:
    bthread::Mutex mtx_;
    bthread::ConditionVariable cv_;
    bool finished_;

    remote::CommonResult result_;
};

struct SyncReadClusterConfigData
{
    SyncReadClusterConfigData(
        std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
            &ng_configs,
        uint64_t &version,
        bool &uninitialized)
        : ng_configs_(ng_configs),
          version_(version),
          uninitialized_(uninitialized)
    {
    }

    void Notify()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        finished_ = true;
        cv_.notify_one();
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        while (!finished_)
        {
            cv_.wait(lk);
        }
    }

    std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
        &ng_configs_;
    uint64_t &version_;
    bool &uninitialized_;
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
    bool finished_{false};
    bool has_error_{false};
};

struct SyncPutAllData
{
    void Finish(const remote::CommonResult &res)
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        if (result_.error_code() == remote::DataStoreError::NO_ERROR)
        {
            result_.set_error_code(res.error_code());
            result_.set_error_msg(res.error_msg());
        }

        --unfinished_request_cnt_;
        if (all_request_started_ && unfinished_request_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    // NOTICE: "unfinished_request_cnt_" must use signed integer.
    int32_t unfinished_request_cnt_{0};
    bool all_request_started_{false};
    remote::CommonResult result_;
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
};

void SyncCallback(void *data,
                  ::google::protobuf::Closure *closure,
                  DataStoreServiceClient &client,
                  const remote::CommonResult &result);

void SyncReadClusterConfigCallback(void *data,
                                   ::google::protobuf::Closure *closure,
                                   DataStoreServiceClient &client,
                                   const remote::CommonResult &result);

struct ReadBaseForArchiveCallbackData
{
    ReadBaseForArchiveCallbackData(bthread::Mutex &mtx,
                                   bthread::ConditionVariable &cv,
                                   size_t &flying_read_cnt,
                                   int &error_code)
        : mtx_(mtx),
          cv_(cv),
          flying_read_cnt_(flying_read_cnt),
          error_code_(error_code),
          partition_id_(0),
          key_str_(),
          value_str_(),
          ts_(0),
          ttl_(0)
    {
    }

    void ResetResult()
    {
        partition_id_ = 0;
        key_str_ = "";
        value_str_ = "";
        ts_ = 0;
        ttl_ = 0;
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        while (flying_read_cnt_ > 0)
        {
            cv_.wait(lk);
        }
    }

    size_t AddFlyingReadCount()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        flying_read_cnt_++;
        return flying_read_cnt_;
    }

    size_t DecreaseFlyingReadCount()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        flying_read_cnt_--;
        if (flying_read_cnt_ == 0)
        {
            cv_.notify_one();
        }
        return flying_read_cnt_;
    }

    size_t GetFlyingReadCount()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        return flying_read_cnt_;
    }

    void AddResult(uint32_t partition_id,
                   const std::string_view key,
                   std::string &&value,
                   uint64_t ts,
                   uint64_t ttl)
    {
        partition_id_ = partition_id;
        key_str_ = key;
        value_str_ = std::move(value);
        ts_ = ts;
        ttl_ = ttl;
    }

    void SetErrorCode(int error_code)
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        // only set the first error code
        if (error_code_ == 0)
        {
            error_code_ = error_code;
        }
    }

    int GetErrorCode()
    {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        return error_code_;
    }

    bthread::Mutex &mtx_;
    bthread::ConditionVariable &cv_;
    size_t &flying_read_cnt_;
    int &error_code_;
    uint32_t partition_id_;
    std::string_view key_str_;
    std::string value_str_;
    uint64_t ts_;
    uint64_t ttl_;
};

void SyncBatchReadForArchiveCallback(void *data,
                                     ::google::protobuf::Closure *closure,
                                     DataStoreServiceClient &client,
                                     const remote::CommonResult &result);

class LoadRangeSliceCallbackData : public Poolable
{
public:
    LoadRangeSliceCallbackData() = default;

    void Reset(std::string_view kv_table_name,
               uint32_t range_partition_id,
               txservice::FillStoreSliceCc *fill_store_slice_req,
               const std::string &&last_key,
               const std::string &&end_key,
               const std::string &session_id,
               size_t batch_size,
               std::shared_ptr<void> defer_unpin)
    {
        kv_table_name_ = kv_table_name;
        range_partition_id_ = range_partition_id;
        fill_store_slice_req_ = fill_store_slice_req;
        last_key_ = std::move(last_key);
        end_key_ = std::move(end_key);
        sesssion_id_ = session_id;
        batch_size_ = batch_size;
        defer_unpin_ = defer_unpin;
    }

    void Clear() override
    {
        kv_table_name_ = "";
        range_partition_id_ = 0;
        fill_store_slice_req_ = nullptr;
        last_key_ = "";
        sesssion_id_ = "";
        batch_size_ = 0;
        defer_unpin_ = nullptr;
    }

    std::string_view kv_table_name_;
    uint32_t range_partition_id_;
    txservice::FillStoreSliceCc *fill_store_slice_req_;
    std::string last_key_;
    std::string end_key_;
    std::string sesssion_id_;
    size_t batch_size_{0};
    std::shared_ptr<void> defer_unpin_;
};

void LoadRangeSliceCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result);

class ReadClosure : public ::google::protobuf::Closure, public Poolable
{
public:
    ReadClosure() = default;

    ReadClosure(const ReadClosure &rhs) = delete;
    ReadClosure(ReadClosure &&rhs) = delete;

    void Reset(DataStoreServiceClient *client,
               const std::string_view table_name,
               const uint32_t partition_id,
               const std::string_view key,
               void *callback_data,
               DataStoreCallback callback)
    {
        is_local_request_ = true;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        table_name_ = table_name;
        partition_id_ = partition_id;
        key_ = key;
        ds_service_client_ = client;
        callback_data_ = callback_data;
        callback_ = callback;
    }

    void Clear() override
    {
        is_local_request_ = false;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        ds_service_client_ = nullptr;
        cntl_.Reset();
        request_.Clear();
        response_.Clear();
        cntl_.Reset();
        channel_.reset();
        table_name_ = "";
        partition_id_ = 0;
        key_ = "";
        result_.Clear();
        value_.clear();
        ts_ = 0;
        callback_ = nullptr;
        callback_data_ = nullptr;
    }

    void PrepareRequest(const bool is_local_request)
    {
        if (is_local_request)
        {
            is_local_request_ = true;
            result_.Clear();
        }
        else
        {
            is_local_request_ = false;
            response_.Clear();
            cntl_.Reset();
            if (rpc_request_prepare_)
            {
                return;
            }
            request_.Clear();
            request_.set_kv_table_name(table_name_.data(), table_name_.size());
            request_.set_partition_id(partition_id_);
            request_.set_key_str(key_.data(), key_.size());
            rpc_request_prepare_ = true;
        }
    }

    // Run() will be called when rpc request is processed by cc node
    // service.
    void Run() override
    {
        PoolableGuard self_guard = PoolableGuard(this);

        const ::EloqDS::remote::CommonResult *result;
        if (!is_local_request_)
        {
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for Read RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    channel_ = ds_service_client_
                                   ->UpdateDataStoreServiceChannelByPartitionId(
                                       partition_id_);

                    // Retry
                    if (retry_count_ < ds_service_client_->retry_limit_)
                    {
                        self_guard.Release();
                        channel_ = nullptr;
                        retry_count_++;
                        ds_service_client_->ReadInternal(this);
                        return;
                    }
                }

                ::EloqDS::remote::CommonResult result;
                result.set_error_code(
                    EloqDS::remote::DataStoreError::NETWORK_ERROR);
                result.set_error_msg(cntl_.ErrorText());
                (*callback_)(callback_data_, this, *ds_service_client_, result);
                channel_ = nullptr;
                return;
            }
            result = &response_.result();
        }
        else
        {
            // local request
            result = &result_;
        }

        auto err_code =
            static_cast<::EloqDS::remote::DataStoreError>(result->error_code());

        if (err_code ==
            ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_client_->HandleShardingError(*result);
            // Retry
            if (retry_count_ < ds_service_client_->retry_limit_)
            {
                self_guard.Release();
                channel_ = nullptr;
                response_.Clear();
                cntl_.Reset();
                retry_count_++;
                ds_service_client_->ReadInternal(this);
                return;
            }
        }

        (*callback_)(callback_data_, this, *ds_service_client_, *result);
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    EloqDS::remote::ReadResponse *ReadResponse()
    {
        return &response_;
    }

    EloqDS::remote::ReadRequest *ReadRequest()
    {
        return &request_;
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    brpc::Channel *Channel()
    {
        return channel_.get();
    }

    const std::string_view TableName()
    {
        return table_name_;
    }

    uint32_t PartitionId()
    {
        return partition_id_;
    }

    const std::string_view Key()
    {
        return key_;
    }

    std::string &LocalValueRef()
    {
        return value_;
    }

    uint64_t &LocalTsRef()
    {
        return ts_;
    }

    uint64_t &LocalTtlRef()
    {
        return ttl_;
    }

    ::EloqDS::remote::CommonResult &LocalResultRef()
    {
        return result_;
    }

    const std::string_view Value() const
    {
        if (is_local_request_)
        {
            return value_;
        }
        else
        {
            return response_.value();
        }
    }

    const std::string &ValueString() const
    {
        if (is_local_request_)
        {
            return value_;
        }
        else
        {
            return response_.value();
        }
    }

    std::string &ValueStringRef()
    {
        if (is_local_request_)
        {
            return value_;
        }
        else
        {
            return *response_.mutable_value();
        }
    }

    uint64_t Ts() const
    {
        if (is_local_request_)
        {
            return ts_;
        }
        else
        {
            return response_.ts();
        }
    }

    uint64_t Ttl() const
    {
        if (is_local_request_)
        {
            return ttl_;
        }
        else
        {
            return response_.ttl();
        }
    }

    ::EloqDS::remote::CommonResult &Result()
    {
        if (is_local_request_)
        {
            return result_;
        }
        else
        {
            return *response_.mutable_result();
        }
    }

    bool IsLocalRequest() const
    {
        return is_local_request_;
    }

private:
    bool is_local_request_{false};
    bool rpc_request_prepare_{false};
    uint16_t retry_count_{0};
    DataStoreServiceClient *ds_service_client_;

    // serve rpc call
    brpc::Controller cntl_;
    EloqDS::remote::ReadRequest request_;
    EloqDS::remote::ReadResponse response_;
    // keep channel alive
    std::shared_ptr<brpc::Channel> channel_;

    // serve local call
    std::string_view table_name_;
    uint32_t partition_id_;
    std::string_view key_;
    ::EloqDS::remote::CommonResult result_;
    std::string value_;
    uint64_t ts_;
    uint64_t ttl_;

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;
};

class FlushDataClosure : public ::google::protobuf::Closure, public Poolable
{
public:
    FlushDataClosure() = default;
    FlushDataClosure(const FlushDataClosure &rhs) = delete;
    FlushDataClosure(FlushDataClosure &&rhs) = delete;

    void Clear() override
    {
        cntl_.Reset();
        request_.Clear();
        response_.Clear();
        cntl_.Reset();
        channel_ = nullptr;
        ds_service_client_ = nullptr;
        retry_count_ = 0;
        is_local_request_ = false;
        rpc_request_prepare_ = false;
        result_.Clear();

        // callback function
        callback_ = nullptr;
        callback_data_ = nullptr;
    }

    void Reset(DataStoreServiceClient &store_hd,
               const std::vector<std::string> *kv_table_names,
               std::vector<uint32_t> &&shard_ids,
               void *callback_data,
               DataStoreCallback callback)
    {
        is_local_request_ = true;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        ds_service_client_ = &store_hd;
        kv_table_names_ = kv_table_names;
        shard_ids_ = std::move(shard_ids);
        callback_data_ = callback_data;
        callback_ = callback;
    }

    void PrepareRequest(const bool is_local_request)
    {
        if (is_local_request)
        {
            is_local_request_ = true;
            result_.Clear();
        }
        else
        {
            is_local_request_ = false;
            response_.Clear();
            cntl_.Reset();
            if (rpc_request_prepare_)
            {
                return;
            }
            request_.Clear();
            for (const std::string &kv_table_name : *kv_table_names_)
            {
                request_.add_kv_table_name(kv_table_name.data(),
                                           kv_table_name.size());
            }

            request_.set_shard_id(shard_ids_.back());
            rpc_request_prepare_ = true;
        }
    }

    // Run() will be called when rpc request is processed by cc node
    // service.
    void Run() override
    {
        PoolableGuard self_guard = PoolableGuard(this);
        const ::EloqDS::remote::CommonResult *result;
        if (!is_local_request_)
        {
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for DeleteRange RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    channel_ = ds_service_client_
                                   ->UpdateDataStoreServiceChannelByShardId(
                                       shard_ids_.back());

                    // Retry
                    if (retry_count_ < ds_service_client_->retry_limit_)
                    {
                        self_guard.Release();
                        channel_ = nullptr;
                        retry_count_++;
                        ds_service_client_->FlushDataInternal(this);
                        return;
                    }
                }

                result_.set_error_code(
                    EloqDS::remote::DataStoreError::NETWORK_ERROR);
                result_.set_error_msg(cntl_.ErrorText());
                (*callback_)(
                    callback_data_, this, *ds_service_client_, result_);
                return;
            }
            result = &response_.result();
        }
        else
        {
            result = &result_;
        }

        auto err_code =
            static_cast<::EloqDS::remote::DataStoreError>(result->error_code());

        if (err_code ==
            ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_client_->HandleShardingError(*result);
            // Retry
            if (retry_count_ < ds_service_client_->retry_limit_)
            {
                self_guard.Release();
                channel_ = nullptr;
                response_.Clear();
                cntl_.Reset();
                retry_count_++;
                ds_service_client_->FlushDataInternal(this);
                return;
            }
        }

        if (err_code == ::EloqDS::remote::DataStoreError::NO_ERROR)
        {
            // flush data to next shard
            assert(shard_ids_.size() > 0);
            shard_ids_.pop_back();

            if (!shard_ids_.empty())
            {
                self_guard.Release();
                retry_count_ = 0;
                rpc_request_prepare_ = false;
                ds_service_client_->FlushDataInternal(this);
                return;
            }
        }

        (*callback_)(callback_data_, this, *ds_service_client_, *result);
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    ::EloqDS::remote::FlushDataRequest *FlushDataRequest()
    {
        return &request_;
    }

    ::EloqDS::remote::FlushDataResponse *FlushDataResponse()
    {
        return &response_;
    }

    brpc::Channel *GetChannel()
    {
        return channel_.get();
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    const std::vector<std::string> &KvTableNames()
    {
        return *kv_table_names_;
    }

    ::EloqDS::remote::CommonResult &LocalResultRef()
    {
        return result_;
    }

    ::EloqDS::remote::CommonResult &Result()
    {
        if (is_local_request_)
        {
            return result_;
        }
        else
        {
            return *response_.mutable_result();
        }
    }

    std::vector<uint32_t> &UnfinishedShards()
    {
        return shard_ids_;
    }

private:
    brpc::Controller cntl_;
    ::EloqDS::remote::FlushDataRequest request_;
    ::EloqDS::remote::FlushDataResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    DataStoreServiceClient *ds_service_client_;
    uint16_t retry_count_{0};

    // call parameters
    bool is_local_request_{false};
    bool rpc_request_prepare_{false};
    const std::vector<std::string> *kv_table_names_{nullptr};
    ::EloqDS::remote::CommonResult result_;
    std::vector<uint32_t> shard_ids_;

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;
};

class DeleteRangeClosure : public ::google::protobuf::Closure, public Poolable
{
public:
    DeleteRangeClosure() = default;
    DeleteRangeClosure(const DeleteRangeClosure &rhs) = delete;
    DeleteRangeClosure(DeleteRangeClosure &&rhs) = delete;

    void Clear() override
    {
        cntl_.Reset();
        request_.Clear();
        response_.Clear();
        cntl_.Reset();
        channel_ = nullptr;
        ds_service_client_ = nullptr;
        retry_count_ = 0;
        is_local_request_ = false;
        rpc_request_prepare_ = false;

        // callback function
        callback_ = nullptr;
        callback_data_ = nullptr;
    }

    void Reset(DataStoreServiceClient &store_hd,
               const std::string_view table_name,
               const uint32_t partition_id,
               const std::string &start_key,
               const std::string &end_key,
               const bool skip_wal,
               void *callback_data,
               DataStoreCallback callback)
    {
        is_local_request_ = true;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        ds_service_client_ = &store_hd;
        table_name_ = table_name;
        partition_id_ = partition_id;
        start_key_ = start_key;
        end_key_ = end_key;
        skip_wal_ = skip_wal;
        callback_data_ = callback_data;
        callback_ = callback;
    }

    void PrepareRequest(const bool is_local_request)
    {
        if (is_local_request)
        {
            is_local_request_ = true;
            result_.Clear();
        }
        else
        {
            is_local_request_ = false;
            response_.Clear();
            cntl_.Reset();
            if (rpc_request_prepare_)
            {
                return;
            }
            // prepare rpc request parameters
            request_.Clear();
            request_.set_kv_table_name(table_name_.data(), table_name_.size());
            request_.set_partition_id(partition_id_);
            request_.set_start_key(start_key_.data(), start_key_.size());
            request_.set_end_key(end_key_.data(), end_key_.size());
            request_.set_skip_wal(skip_wal_);
            rpc_request_prepare_ = true;
        }
    }

    // Run() will be called when rpc request is processed by cc node
    // service.
    void Run() override
    {
        PoolableGuard self_guard = PoolableGuard(this);
        const ::EloqDS::remote::CommonResult *result;
        if (!is_local_request_)
        {
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for DeleteRange RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    channel_ = ds_service_client_
                                   ->UpdateDataStoreServiceChannelByPartitionId(
                                       partition_id_);

                    // Retry
                    if (retry_count_ < ds_service_client_->retry_limit_)
                    {
                        self_guard.Release();
                        channel_ = nullptr;
                        retry_count_++;
                        ds_service_client_->DeleteRangeInternal(this);
                        return;
                    }
                }

                result_.set_error_code(
                    EloqDS::remote::DataStoreError::NETWORK_ERROR);
                result_.set_error_msg(cntl_.ErrorText());
                (*callback_)(
                    callback_data_, this, *ds_service_client_, result_);
                return;
            }
            result = &response_.result();
        }
        else
        {
            result = &result_;
        }

        auto err_code =
            static_cast<::EloqDS::remote::DataStoreError>(result->error_code());

        if (err_code ==
            ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_client_->HandleShardingError(*result);
            // Retry
            if (retry_count_ < ds_service_client_->retry_limit_)
            {
                self_guard.Release();
                channel_ = nullptr;
                response_.Clear();
                cntl_.Reset();
                retry_count_++;
                ds_service_client_->DeleteRangeInternal(this);
                return;
            }
        }

        (*callback_)(callback_data_, this, *ds_service_client_, *result);
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    brpc::Channel *GetChannel()
    {
        return channel_.get();
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    ::EloqDS::remote::DeleteRangeRequest *DeleteRangeRequest()
    {
        return &request_;
    }

    ::EloqDS::remote::DeleteRangeResponse *DeleteRangeResponse()
    {
        return &response_;
    }

    const std::string_view TableName()
    {
        return table_name_;
    }

    uint32_t PartitionId()
    {
        return partition_id_;
    }

    std::string_view StartKey()
    {
        return start_key_;
    }

    std::string_view EndKey()
    {
        return end_key_;
    }

    bool SkipWal() const
    {
        return skip_wal_;
    }

    ::EloqDS::remote::CommonResult &LocalResultRef()
    {
        return result_;
    }

    ::EloqDS::remote::CommonResult &Result()
    {
        if (is_local_request_)
        {
            return result_;
        }
        else
        {
            return *response_.mutable_result();
        }
    }

private:
    brpc::Controller cntl_;
    ::EloqDS::remote::DeleteRangeRequest request_;
    ::EloqDS::remote::DeleteRangeResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    DataStoreServiceClient *ds_service_client_;
    uint16_t retry_count_{0};

    // call parameters
    bool is_local_request_{false};
    bool rpc_request_prepare_{false};
    std::string_view table_name_;
    uint32_t partition_id_;
    std::string_view start_key_;
    std::string_view end_key_;
    bool skip_wal_{false};
    ::EloqDS::remote::CommonResult result_;

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;
};

class DropTableClosure : public ::google::protobuf::Closure, public Poolable
{
public:
    DropTableClosure() = default;
    DropTableClosure(const DropTableClosure &rhs) = delete;
    DropTableClosure(DropTableClosure &&rhs) = delete;

    void Clear() override
    {
        cntl_.Reset();
        request_.Clear();
        response_.Clear();
        cntl_.Reset();
        channel_ = nullptr;
        ds_service_client_ = nullptr;
        retry_count_ = 0;
        is_local_request_ = false;
        rpc_request_prepare_ = false;
        result_.Clear();

        // callback function
        callback_ = nullptr;
        callback_data_ = nullptr;
    }

    void Reset(DataStoreServiceClient &store_hd,
               std::string_view table_name,
               std::vector<uint32_t> &&shard_ids,
               void *callback_data,
               DataStoreCallback callback)
    {
        is_local_request_ = true;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        ds_service_client_ = &store_hd;
        table_name_ = table_name;
        shard_ids_ = std::move(shard_ids);
        callback_data_ = callback_data;
        callback_ = callback;
    }

    void PrepareRequest(const bool is_local_request)
    {
        if (is_local_request)
        {
            is_local_request_ = true;
            result_.Clear();
        }
        else
        {
            is_local_request_ = false;
            response_.Clear();
            cntl_.Reset();
            if (rpc_request_prepare_)
            {
                return;
            }
            request_.Clear();
            request_.set_kv_table_name(table_name_.data(), table_name_.size());
            request_.set_shard_id(shard_ids_.back());
            rpc_request_prepare_ = true;
        }
    }

    // Run() will be called when rpc request is processed by cc node
    // service.
    void Run() override
    {
        PoolableGuard self_guard = PoolableGuard(this);
        const ::EloqDS::remote::CommonResult *result;
        if (!is_local_request_)
        {
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for DropTable RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    channel_ = ds_service_client_
                                   ->UpdateDataStoreServiceChannelByShardId(
                                       shard_ids_.back());

                    // Retry
                    if (retry_count_ < ds_service_client_->retry_limit_)
                    {
                        self_guard.Release();
                        channel_ = nullptr;
                        retry_count_++;
                        ds_service_client_->DropTableInternal(this);
                        return;
                    }
                }

                result_.set_error_code(
                    EloqDS::remote::DataStoreError::NETWORK_ERROR);
                result_.set_error_msg(cntl_.ErrorText());
                (*callback_)(
                    callback_data_, this, *ds_service_client_, result_);
                return;
            }
            result = &response_.result();
        }
        else
        {
            result = &result_;
        }

        auto err_code =
            static_cast<::EloqDS::remote::DataStoreError>(result->error_code());

        if (err_code ==
            ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_client_->HandleShardingError(*result);
            // Retry
            if (retry_count_ < ds_service_client_->retry_limit_)
            {
                self_guard.Release();
                channel_ = nullptr;
                response_.Clear();
                cntl_.Reset();
                retry_count_++;
                ds_service_client_->DropTableInternal(this);
                return;
            }
        }

        if (err_code == ::EloqDS::remote::DataStoreError::NO_ERROR)
        {
            assert(shard_ids_.size() > 0);
            shard_ids_.pop_back();

            // flush data to next shard
            if (!shard_ids_.empty())
            {
                self_guard.Release();
                retry_count_ = 0;
                rpc_request_prepare_ = false;
                ds_service_client_->DropTableInternal(this);
                return;
            }
        }

        (*callback_)(callback_data_, this, *ds_service_client_, *result);
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    ::EloqDS::remote::DropTableRequest *DropTableRequest()
    {
        return &request_;
    }

    ::EloqDS::remote::DropTableResponse *DropTableResponse()
    {
        return &response_;
    }

    brpc::Channel *GetChannel()
    {
        return channel_.get();
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    const std::string_view TableName()
    {
        return table_name_;
    }

    ::EloqDS::remote::CommonResult &LocalResultRef()
    {
        return result_;
    }

    ::EloqDS::remote::CommonResult &Result()
    {
        if (is_local_request_)
        {
            return result_;
        }
        else
        {
            return *response_.mutable_result();
        }
    }

    std::vector<uint32_t> &UnfinishedShards()
    {
        return shard_ids_;
    }

private:
    brpc::Controller cntl_;
    ::EloqDS::remote::DropTableRequest request_;
    ::EloqDS::remote::DropTableResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    DataStoreServiceClient *ds_service_client_;
    uint16_t retry_count_{0};

    // call parameters
    bool is_local_request_{false};
    bool rpc_request_prepare_{false};
    std::string_view table_name_;
    ::EloqDS::remote::CommonResult result_;
    std::vector<uint32_t> shard_ids_;

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;
};

class BatchWriteRecordsClosure : public ::google::protobuf::Closure,
                                 public Poolable
{
public:
    BatchWriteRecordsClosure() = default;
    BatchWriteRecordsClosure(const BatchWriteRecordsClosure &rhs) = delete;
    BatchWriteRecordsClosure(BatchWriteRecordsClosure &&rhs) = delete;

    void Clear() override
    {
        Reset();
    }

    void Reset()
    {
        ds_service_client_ = nullptr;
        retry_count_ = 0;
        req_shard_id_ = 0;
        is_local_request_ = false;

        kv_table_name_ = "";
        partition_id_ = 0;
        key_parts_.clear();
        record_parts_.clear();
        record_ts_.clear();
        record_ttl_.clear();
        op_types_.clear();
        skip_wal_ = false;

        callback_data_ = nullptr;
        callback_ = nullptr;

        request_.Clear();
        response_.Clear();
        cntl_.Reset();
        channel_ = nullptr;
        parts_cnt_per_key_ = 1;
        parts_cnt_per_record_ = 1;
    }

    // for writing single record
    void Reset(DataStoreServiceClient &store_hd,
               std::string_view kv_table_name,
               int32_t partition_id,
               std::string_view key,
               std::string_view record,
               uint64_t record_ts,
               uint64_t record_ttl,
               WriteOpType op_type,
               bool skip_wal,
               void *callback_data,
               DataStoreCallback callback,
               uint16_t key_parts_size,
               uint16_t record_parts_size)
    {
        Reset();

        ds_service_client_ = &store_hd;
        kv_table_name_ = kv_table_name;
        partition_id_ = partition_id;
        key_parts_.emplace_back(key);
        record_parts_.emplace_back(record);
        record_ts_.emplace_back(record_ts);
        record_ttl_.emplace_back(record_ttl);
        op_types_.emplace_back(op_type);
        skip_wal_ = skip_wal;
        callback_data_ = callback_data;
        callback_ = callback;
        parts_cnt_per_key_ = key_parts_size;
        parts_cnt_per_record_ = record_parts_size;
    }

    void Reset(DataStoreServiceClient &store_hd,
               std::string_view kv_table_name,
               int32_t partition_id,
               std::vector<std::string_view> &&key_parts,
               std::vector<std::string_view> &&record_parts,
               std::vector<uint64_t> &&record_ts,
               std::vector<uint64_t> &&record_ttl,
               std::vector<WriteOpType> &&op_types,
               bool skip_wal,
               void *callback_data,
               DataStoreCallback callback,
               uint16_t parts_cnt_per_key,
               uint16_t parts_cnt_per_record)
    {
        Reset();

        ds_service_client_ = &store_hd;
        kv_table_name_ = kv_table_name;
        partition_id_ = partition_id;

        key_parts_ = std::move(key_parts);
        record_parts_ = std::move(record_parts);
        record_ts_ = std::move(record_ts);
        record_ttl_ = std::move(record_ttl);
        op_types_ = std::move(op_types);
        skip_wal_ = skip_wal;

        callback_data_ = callback_data;
        callback_ = callback;

        parts_cnt_per_key_ = parts_cnt_per_key;
        parts_cnt_per_record_ = parts_cnt_per_record;
    }

    void Run() override
    {
        PoolableGuard self_guard = PoolableGuard(this);

        bool need_retry = false;
        if (!is_local_request_)
        {
            assert(channel_ != nullptr);
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for BatchWriteRecords RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    // NOTICE(lzx): Should re-fetch the shared_id after
                    // supporting partition migration between data shards.
                    channel_ = ds_service_client_
                                   ->UpdateDataStoreServiceChannelByShardId(
                                       req_shard_id_);
                    need_retry = true;
                }
                else
                {
                    result_.set_error_code(
                        EloqDS::remote::DataStoreError::NETWORK_ERROR);
                    result_.set_error_msg(cntl_.ErrorText());
                }
            }
            else
            {
                // TODO(lzx): handle error.
                result_ = response_.result();
            }
        }
        else
        {
            auto err_code = static_cast<::EloqDS::remote::DataStoreError>(
                result_.error_code());

            if (err_code ==
                ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
            {
                ds_service_client_->HandleShardingError(result_);
                // TODO(lzx): retry.
            }
        }

        if (need_retry && retry_count_ < 2)
        {
            self_guard.Release();
            retry_count_++;
            ds_service_client_->BatchWriteRecordsInternal(this);
            return;
        }

        (*callback_)(callback_data_, this, *ds_service_client_, result_);
    }

    void PrepareRemoteRequest()
    {
        // clear
        cntl_.Reset();
        cntl_.set_timeout_ms(5000);
        request_.Clear();
        response_.Clear();
        is_local_request_ = false;
        channel_ = nullptr;

        // make request
        request_.set_kv_table_name(kv_table_name_.data(),
                                   kv_table_name_.size());
        request_.set_partition_id(partition_id_);
        request_.set_skip_wal(skip_wal_);
        assert(record_ts_.size() * parts_cnt_per_key_ == key_parts_.size());
        assert(record_ts_.size() * parts_cnt_per_record_ ==
               record_parts_.size());
        // record_ts_.size() is the count of records.
        for (size_t i = 0; i < record_ts_.size(); ++i)
        {
            auto *item = request_.add_items();

            // set key
            std::string *mutable_key = item->mutable_key();
            for (uint16_t j = 0; j < parts_cnt_per_key_; j++)
            {
                size_t key_part_idx = i * parts_cnt_per_key_ + j;
                mutable_key->append(key_parts_[key_part_idx].data(),
                                    key_parts_[key_part_idx].size());
            }

            // set value
            std::string *mutable_value = item->mutable_value();
            for (uint16_t j = 0; j < parts_cnt_per_record_; j++)
            {
                size_t record_part_idx = j + i * parts_cnt_per_record_;
                mutable_value->append(record_parts_[record_part_idx].data(),
                                      record_parts_[record_part_idx].size());
            }

            item->set_ts(record_ts_[i]);
            item->set_ttl(record_ttl_[i]);
            EloqDS::remote::WriteOpType op_type =
                op_types_[i] == WriteOpType::PUT
                    ? EloqDS::remote::WriteOpType::Put
                    : EloqDS::remote::WriteOpType::Delete;
            item->set_op_type(op_type);
        }
    }

    void SetReqShardId(uint32_t shard_id)
    {
        req_shard_id_ = shard_id;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    EloqDS::remote::BatchWriteRecordsRequest *RemoteRequest()
    {
        return &request_;
    }

    EloqDS::remote::BatchWriteRecordsResponse *RemoteResponse()
    {
        return &response_;
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    brpc::Channel *Channel()
    {
        return channel_.get();
    }

    remote::CommonResult &Result()
    {
        return result_;
    }

    uint16_t PartsCountPerKey()
    {
        return parts_cnt_per_key_;
    }

    uint16_t PartsCountPerRecord()
    {
        return parts_cnt_per_record_;
    }

private:
    brpc::Controller cntl_;
    EloqDS::remote::BatchWriteRecordsRequest request_;
    EloqDS::remote::BatchWriteRecordsResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    DataStoreServiceClient *ds_service_client_{nullptr};
    uint32_t req_shard_id_{0};
    uint16_t retry_count_{0};

    std::string_view kv_table_name_;
    int32_t partition_id_;
    std::vector<std::string_view> key_parts_;
    std::vector<std::string_view> record_parts_;
    std::vector<uint64_t> record_ts_;
    std::vector<uint64_t> record_ttl_;
    std::vector<WriteOpType> op_types_;

    bool skip_wal_{false};
    remote::CommonResult result_;

    // TODO(lzx): check if "onflying_req_count_" is needed?
    // std::atomic<uint64_t> &onflying_req_count_;
    bool is_local_request_{false};

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;

    uint16_t parts_cnt_per_key_;
    uint16_t parts_cnt_per_record_;

    friend class DataStoreServiceClient;
};

class ScanNextClosure : public ::google::protobuf::Closure, public Poolable
{
public:
    ScanNextClosure() = default;
    ScanNextClosure(const ScanNextClosure &rhs) = delete;
    ScanNextClosure(ScanNextClosure &&rhs) = delete;

    void Clear() override
    {
        cntl_.Reset();
        request_.Clear();
        response_.Clear();
        cntl_.Reset();
        channel_ = nullptr;
        ds_service_client_ = nullptr;
        retry_count_ = 0;
        is_local_request_ = false;
        rpc_request_prepare_ = false;
        table_name_ = "";
        partition_id_ = 0;
        start_key_ = "";
        end_key_ = "";
        inclusive_start_ = false;
        inclusive_end_ = false;
        scan_forward_ = true;
        session_id_ = "";
        batch_size_ = 0;
        search_conditions_ = nullptr;
        result_.Clear();
        items_.clear();

        // callback function
        callback_ = nullptr;
        callback_data_ = nullptr;
    }

    void Reset(DataStoreServiceClient &store_hd,
               const std::string_view table_name,
               uint32_t partition_id,
               const std::string_view start_key,
               const std::string_view end_key,
               bool inclusive_start,
               bool inclusive_end,
               bool scan_forward,
               const std::string_view session_id,
               const uint32_t batch_size,
               const std::vector<remote::SearchCondition> *search_conditions,
               void *callback_data,
               DataStoreCallback callback)
    {
        is_local_request_ = true;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        ds_service_client_ = &store_hd;
        table_name_ = table_name;
        partition_id_ = partition_id;
        start_key_ = start_key;
        end_key_ = end_key;
        inclusive_start_ = inclusive_start;
        inclusive_end_ = inclusive_end;
        scan_forward_ = scan_forward;
        session_id_ = session_id;
        batch_size_ = batch_size;
        search_conditions_ = search_conditions;
        callback_data_ = callback_data;
        callback_ = callback;
        assert(callback_ != nullptr);
    }

    void PrepareRequest(const bool is_local_request)
    {
        if (is_local_request)
        {
            is_local_request_ = true;
            result_.Clear();
        }
        else
        {
            is_local_request_ = false;
            // session id is volatile, so we need to set it every time
            request_.set_session_id(session_id_);
            response_.Clear();
            cntl_.Reset();
            if (rpc_request_prepare_)
            {
                return;
            }
            // prepare rpc request parameters
            request_.Clear();
            request_.set_kv_table_name_str(table_name_.data(),
                                           table_name_.size());
            request_.set_partition_id(partition_id_);
            request_.set_start_key(start_key_.data(), start_key_.size());
            request_.set_inclusive_start(inclusive_start_);
            request_.set_inclusive_end(inclusive_end_);
            request_.set_end_key(end_key_.data(), end_key_.size());
            request_.set_scan_forward(scan_forward_);
            request_.set_session_id(session_id_);
            request_.set_batch_size(batch_size_);
            if (search_conditions_)
            {
                for (auto &cond : *search_conditions_)
                {
                    remote::SearchCondition *rcond =
                        request_.add_search_conditions();
                    rcond->set_field_name(cond.field_name());
                    rcond->set_op(cond.op());
                    rcond->set_value(cond.value());
                }
                assert(static_cast<size_t>(request_.search_conditions_size()) ==
                       search_conditions_->size());
            }
            rpc_request_prepare_ = true;
        }
    }

    // Run() will be called when rpc request is processed by cc node
    // service.
    void Run() override
    {
        PoolableGuard self_guard(this);
        const ::EloqDS::remote::CommonResult *result;
        if (!is_local_request_)
        {
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for ScanNext RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    channel_ = ds_service_client_
                                   ->UpdateDataStoreServiceChannelByPartitionId(
                                       partition_id_);

                    // Retry
                    if (retry_count_ < ds_service_client_->retry_limit_)
                    {
                        self_guard.Release();
                        channel_ = nullptr;
                        retry_count_++;
                        ds_service_client_->ScanNextInternal(this);
                        return;
                    }
                }

                result_.set_error_code(
                    EloqDS::remote::DataStoreError::NETWORK_ERROR);
                result_.set_error_msg(cntl_.ErrorText());
                (*callback_)(
                    callback_data_, this, *ds_service_client_, result_);
                return;
            }
            result = &response_.result();
        }
        else
        {
            result = &result_;
        }

        auto err_code =
            static_cast<::EloqDS::remote::DataStoreError>(result->error_code());

        if (err_code ==
            ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_client_->HandleShardingError(*result);
            // Retry
            if (retry_count_ < ds_service_client_->retry_limit_)
            {
                self_guard.Release();
                channel_ = nullptr;
                response_.Clear();
                cntl_.Reset();
                retry_count_++;
                ds_service_client_->ScanNextInternal(this);
                return;
            }
        }

        (*callback_)(callback_data_, this, *ds_service_client_, *result);
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    brpc::Channel *GetChannel()
    {
        return channel_.get();
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    ::EloqDS::remote::ScanRequest *ScanNextRequest()
    {
        return &request_;
    }

    ::EloqDS::remote::ScanResponse *ScanNextResponse()
    {
        return &response_;
    }

    const std::string_view TableName()
    {
        return table_name_;
    }

    uint32_t PartitionId()
    {
        return partition_id_;
    }

    const std::string_view StartKey()
    {
        return start_key_;
    }

    const std::string_view EndKey()
    {
        return end_key_;
    }

    bool InclusiveStart()
    {
        return inclusive_start_;
    }

    bool InclusiveEnd()
    {
        return inclusive_end_;
    }

    bool ScanForward()
    {
        return scan_forward_;
    }

    uint32_t BatchSize()
    {
        return batch_size_;
    }

    std::string &LocalSessionIdRef()
    {
        return session_id_;
    }

    const std::string &SessionId() const
    {
        if (is_local_request_)
        {
            return session_id_;
        }
        else
        {
            return response_.session_id();
        }
    }

    ::EloqDS::remote::CommonResult &LocalResultRef()
    {
        return result_;
    }

    ::EloqDS::remote::CommonResult &Result()
    {
        if (is_local_request_)
        {
            return result_;
        }
        else
        {
            return *response_.mutable_result();
        }
    }

    uint32_t ItemsSize()
    {
        if (is_local_request_)
        {
            return items_.size();
        }
        else
        {
            return response_.items_size();
        }
    }

    void GetItem(uint32_t idx,
                 std::string &key,
                 std::string &value,
                 uint64_t &ts,
                 uint64_t &ttl)
    {
        if (is_local_request_)
        {
            key = std::move(items_[idx].key_);
            value = std::move(items_[idx].value_);
            ts = items_[idx].ts_;
            ttl = items_[idx].ttl_;
        }
        else
        {
            key = response_.items(idx).key();
            value = response_.items(idx).value();
            ts = response_.items(idx).ts();
            ttl = response_.items(idx).ttl();
        }
    }

    std::vector<ScanTuple> &LocalItemsRef()
    {
        return items_;
    }

    const std::string_view GetSessionId() const
    {
        if (is_local_request_)
        {
            return session_id_;
        }
        else
        {
            return response_.session_id();
        }
    }

    const std::vector<remote::SearchCondition> *LocalSearchConditionsPtr()
    {
        return search_conditions_;
    }

private:
    brpc::Controller cntl_;
    ::EloqDS::remote::ScanRequest request_;
    ::EloqDS::remote::ScanResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    DataStoreServiceClient *ds_service_client_;
    uint16_t retry_count_{0};

    // call parameters
    bool is_local_request_{false};
    bool rpc_request_prepare_{false};
    std::string_view table_name_;
    uint32_t partition_id_;
    std::string_view start_key_;
    std::string_view end_key_;
    bool inclusive_start_{false};
    bool inclusive_end_{false};
    bool scan_forward_{true};
    std::string session_id_;
    uint32_t batch_size_;
    const std::vector<remote::SearchCondition> *search_conditions_;

    // reuslt
    ::EloqDS::remote::CommonResult result_;
    std::vector<ScanTuple> items_;

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;
};

class CreateSnapshotForBackupClosure : public ::google::protobuf::Closure,
                                       public Poolable
{
public:
    CreateSnapshotForBackupClosure() = default;
    CreateSnapshotForBackupClosure(const CreateSnapshotForBackupClosure &rhs) =
        delete;
    CreateSnapshotForBackupClosure(CreateSnapshotForBackupClosure &&rhs) =
        delete;

    void Clear() override
    {
        cntl_.Reset();
        request_.Clear();
        response_.Clear();
        channel_ = nullptr;
        ds_service_client_ = nullptr;
        retry_count_ = 0;
        is_local_request_ = false;
        rpc_request_prepare_ = false;
        result_.Clear();

        shard_ids_.clear();
        backup_name_ = "";
        backup_ts_ = 0;
        backup_files_ = nullptr;

        // callback function
        callback_ = nullptr;
        callback_data_ = nullptr;
    }

    void Reset(DataStoreServiceClient &store_hd,
               std::vector<uint32_t> &&shard_ids,
               std::string_view backup_name,
               uint64_t backup_ts,
               std::vector<std::string> *backup_files,
               void *callback_data,
               DataStoreCallback callback)
    {
        is_local_request_ = true;
        rpc_request_prepare_ = false;
        retry_count_ = 0;
        ds_service_client_ = &store_hd;
        shard_ids_ = std::move(shard_ids);
        backup_name_ = backup_name;
        backup_ts_ = backup_ts;
        backup_files_ = backup_files;
        callback_data_ = callback_data;
        callback_ = callback;
    }

    void PrepareRequest(const bool is_local_request)
    {
        if (is_local_request)
        {
            is_local_request_ = true;
            result_.Clear();
        }
        else
        {
            is_local_request_ = false;
            response_.Clear();
            cntl_.Reset();
            if (rpc_request_prepare_)
            {
                return;
            }
            request_.Clear();
            // prepare rpc request parameters

            request_.set_shard_id(shard_ids_.back());
            request_.set_backup_name(backup_name_.data(), backup_name_.size());
            request_.set_backup_ts(backup_ts_);
            rpc_request_prepare_ = true;
        }
    }

    bool IsLocalRequest() const
    {
        return is_local_request_;
    }

    // Run() will be called when rpc request is processed by cc node
    // service.
    void Run() override
    {
        PoolableGuard self_guard = PoolableGuard(this);
        const ::EloqDS::remote::CommonResult *result;
        if (!is_local_request_)
        {
            if (cntl_.Failed())
            {
                // RPC failed.
                LOG(ERROR) << "Failed for CreateSnapshotForBackup RPC request "
                           << ", with Error code: " << cntl_.ErrorCode()
                           << ". Error Msg: " << cntl_.ErrorText();
                if (cntl_.ErrorCode() != brpc::EOVERCROWDED &&
                    cntl_.ErrorCode() != EAGAIN &&
                    cntl_.ErrorCode() != brpc::ERPCTIMEDOUT)
                {
                    uint32_t shard_id = shard_ids_.back();
                    channel_ =
                        ds_service_client_
                            ->UpdateDataStoreServiceChannelByShardId(shard_id);

                    // Retry
                    if (retry_count_ < ds_service_client_->retry_limit_)
                    {
                        self_guard.Release();
                        channel_ = nullptr;
                        retry_count_++;
                        ds_service_client_->CreateSnapshotForBackupInternal(
                            this);
                        return;
                    }
                }
                result_.set_error_code(
                    EloqDS::remote::DataStoreError::NETWORK_ERROR);
                result_.set_error_msg(cntl_.ErrorText());
                (*callback_)(
                    callback_data_, this, *ds_service_client_, result_);
                return;
            }
            result = &response_.result();
        }
        else
        {
            result = &result_;
        }
        auto err_code =
            static_cast<::EloqDS::remote::DataStoreError>(result->error_code());
        if (err_code ==
            ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER)
        {
            ds_service_client_->HandleShardingError(*result);
            // Retry
            if (retry_count_ < ds_service_client_->retry_limit_)
            {
                self_guard.Release();
                channel_ = nullptr;
                response_.Clear();
                cntl_.Reset();
                retry_count_++;
                ds_service_client_->CreateSnapshotForBackupInternal(this);
                return;
            }
        }

        (*callback_)(callback_data_, this, *ds_service_client_, *result);
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    brpc::Channel *GetChannel()
    {
        return channel_.get();
    }

    void SetChannel(std::shared_ptr<brpc::Channel> channel)
    {
        channel_ = channel;
    }

    const std::string_view GetBackupName()
    {
        return backup_name_;
    }

    uint64_t GetBackupTs()
    {
        return backup_ts_;
    }

    std::vector<uint32_t> &UnfinishedShards()
    {
        return shard_ids_;
    }

    std::vector<std::string> *LocalBackupFilesPtr()
    {
        return backup_files_;
    }

    ::EloqDS::remote::CommonResult &LocalResultRef()
    {
        return result_;
    }

    std::vector<std::string> *BackupFiles()
    {
        if (is_local_request_)
        {
            return backup_files_;
        }
        else
        {
            backup_files_->clear();
            for (int i = 0; i < response_.backup_files_size(); i++)
            {
                backup_files_->emplace_back(response_.backup_files(i));
            }
            return backup_files_;
        }
    }

    ::EloqDS::remote::CommonResult &Result()
    {
        if (is_local_request_)
        {
            return result_;
        }
        else
        {
            return *response_.mutable_result();
        }
    }

    ::EloqDS::remote::CreateSnapshotForBackupRequest *RemoteRequest()
    {
        return &request_;
    }

    ::EloqDS::remote::CreateSnapshotForBackupResponse *RemoteResponse()
    {
        return &response_;
    }

private:
    brpc::Controller cntl_;
    ::EloqDS::remote::CreateSnapshotForBackupRequest request_;
    ::EloqDS::remote::CreateSnapshotForBackupResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    DataStoreServiceClient *ds_service_client_;
    uint16_t retry_count_{0};

    // call parameters
    bool is_local_request_{false};
    bool rpc_request_prepare_{false};
    std::string_view backup_name_;
    uint64_t backup_ts_;
    std::vector<std::string> *backup_files_{nullptr};
    std::vector<uint32_t> shard_ids_;
    ::EloqDS::remote::CommonResult result_;

    // callback function
    DataStoreCallback callback_;
    void *callback_data_;
};

void FetchRecordCallback(void *data,
                         ::google::protobuf::Closure *closure,
                         DataStoreServiceClient &client,
                         const remote::CommonResult &result);

void FetchSnapshotCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result);

struct AsyncDropTableCallbackData
{
    std::string kv_table_name_;
};

void AsyncDropTableCallback(void *data,
                            ::google::protobuf::Closure *closure,
                            DataStoreServiceClient &client,
                            const remote::CommonResult &result);

void FetchTableCatalogCallback(void *data,
                               ::google::protobuf::Closure *closure,
                               DataStoreServiceClient &client,
                               const remote::CommonResult &result);

struct FetchTableCallbackData : public SyncCallbackData
{
    FetchTableCallbackData(std::string &schema_image,
                           bool &found,
                           uint64_t &version_ts)
        : schema_image_(schema_image), found_(found), version_ts_(version_ts)
    {
    }

    std::string &schema_image_;
    bool &found_;
    uint64_t &version_ts_;
};

void FetchTableCallback(void *data,
                        ::google::protobuf::Closure *closure,
                        DataStoreServiceClient &client,
                        const remote::CommonResult &result);

void SyncPutAllCallback(void *data,
                        ::google::protobuf::Closure *closure,
                        DataStoreServiceClient &client,
                        const remote::CommonResult &result);

struct FetchDatabaseCallbackData : public SyncCallbackData
{
    FetchDatabaseCallbackData(std::string &definition,
                              bool &found,
                              const std::function<void()> *yield_fptr,
                              const std::function<void()> *resume_fptr)
        : db_definition_(definition),
          found_(found),
          yield_fptr_(yield_fptr),
          resume_fptr_(resume_fptr)
    {
    }

    void Wait() override
    {
        if (yield_fptr_ != nullptr)
        {
            (*yield_fptr_)();
        }
        else
        {
            SyncCallbackData::Wait();
        }
    }

    void Notify() override
    {
        if (resume_fptr_ != nullptr)
        {
            (*resume_fptr_)();
        }
        else
        {
            SyncCallbackData::Notify();
        }
    }

    std::string &db_definition_;
    bool &found_;
    const std::function<void()> *yield_fptr_;
    const std::function<void()> *resume_fptr_;
};

void FetchDatabaseCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result);

struct FetchAllDatabaseCallbackData : public SyncCallbackData
{
    FetchAllDatabaseCallbackData(const std::string_view kv_table_name,
                                 std::vector<std::string> &dbnames,
                                 const std::function<void()> *yield_fptr,
                                 const std::function<void()> *resume_fptr,
                                 int32_t partition_id,
                                 uint32_t batch_size)
        : kv_table_name_(kv_table_name),
          dbnames_(dbnames),
          yield_fptr_(yield_fptr),
          resume_fptr_(resume_fptr),
          search_conds_(),
          session_id_(),
          start_key_(),
          end_key_(),
          partition_id_(partition_id),
          batch_size_(batch_size)
    {
    }

    void Wait() override
    {
        if (yield_fptr_ != nullptr)
        {
            (*yield_fptr_)();
        }
        else
        {
            SyncCallbackData::Wait();
        }
    }

    void Notify() override
    {
        if (resume_fptr_ != nullptr)
        {
            (*resume_fptr_)();
        }
        else
        {
            SyncCallbackData::Notify();
        }
    }

    const std::string_view kv_table_name_;
    std::vector<std::string> &dbnames_;
    const std::function<void()> *yield_fptr_;
    const std::function<void()> *resume_fptr_;

    std::vector<remote::SearchCondition> search_conds_;
    std::string session_id_;
    std::string start_key_;
    std::string end_key_;
    int32_t partition_id_;
    uint32_t batch_size_;
};

void FetchAllDatabaseCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result);

struct DiscoverAllTableNamesCallbackData : public SyncCallbackData
{
    DiscoverAllTableNamesCallbackData(const std::string_view kv_table_name,
                                      std::vector<std::string> &table_names,
                                      const std::function<void()> *yield_fptr,
                                      const std::function<void()> *resume_fptr,
                                      int32_t partition_id,
                                      uint32_t batch_size)
        : kv_table_name_(kv_table_name),
          table_names_(table_names),
          yield_fptr_(yield_fptr),
          resume_fptr_(resume_fptr),
          search_conds_(),
          session_id_(),
          start_key_(),
          end_key_(),
          partition_id_(partition_id),
          batch_size_(batch_size)
    {
    }

    void Wait() override
    {
        if (yield_fptr_ != nullptr)
        {
            (*yield_fptr_)();
        }
        else
        {
            SyncCallbackData::Wait();
        }
    }

    void Notify() override
    {
        if (resume_fptr_ != nullptr)
        {
            (*resume_fptr_)();
        }
        else
        {
            SyncCallbackData::Notify();
        }
    }

    const std::string_view kv_table_name_;
    std::vector<std::string> &table_names_;
    const std::function<void()> *yield_fptr_;
    const std::function<void()> *resume_fptr_;

    std::vector<remote::SearchCondition> search_conds_;
    std::string session_id_;
    std::string start_key_;
    std::string end_key_;
    int32_t partition_id_;
    uint32_t batch_size_;
};

void DiscoverAllTableNamesCallback(void *data,
                                   ::google::protobuf::Closure *closure,
                                   DataStoreServiceClient &client,
                                   const remote::CommonResult &result);

void FetchTableRangesCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result);

void FetchRangeSlicesCallback(void *data,
                              ::google::protobuf::Closure *closure,
                              DataStoreServiceClient &client,
                              const remote::CommonResult &result);
void FetchCurrentTableStatsCallback(void *data,
                                    ::google::protobuf::Closure *closure,
                                    DataStoreServiceClient &client,
                                    const remote::CommonResult &result);

void FetchTableStatsCallback(void *data,
                             ::google::protobuf::Closure *closure,
                             DataStoreServiceClient &client,
                             const remote::CommonResult &result);

struct FetchArchivesCallbackData : public SyncCallbackData
{
    FetchArchivesCallbackData(const std::string_view kv_table_name,
                              uint32_t partition_id,
                              std::string &start_key,
                              const std::string &end_key,
                              const size_t batch_size,
                              const size_t limit,
                              const bool scan_forward)
        : kv_table_name_(kv_table_name),
          partition_id_(partition_id),
          start_key_(start_key),
          end_key_(end_key),
          batch_size_(batch_size),
          limit_(limit),
          scan_forward_(scan_forward),
          session_id_("")
    {
    }

    const std::string_view kv_table_name_;
    const uint32_t partition_id_;
    std::string &start_key_;
    const std::string &end_key_;
    const size_t batch_size_;
    const size_t limit_;
    const bool scan_forward_;
    std::string session_id_;
    std::vector<std::string> archive_values_;
    std::vector<uint64_t> archive_commit_ts_;
};

void FetchArchivesCallback(void *data,
                           ::google::protobuf::Closure *closure,
                           DataStoreServiceClient &client,
                           const remote::CommonResult &result);

struct FetchRecordArchivesCallbackData
{
    FetchRecordArchivesCallbackData(txservice::FetchRecordCc *fetch_cc,
                                    const std::string_view kv_table_name,
                                    uint32_t partition_id,
                                    std::string &&start_key,
                                    std::string &&end_key)
        : fetch_cc_(fetch_cc),
          kv_table_name_(kv_table_name),
          partition_id_(partition_id),
          start_key_(std::move(start_key)),
          end_key_(std::move(end_key)),
          session_id_("")
    {
    }

    txservice::FetchRecordCc *fetch_cc_;
    const std::string_view kv_table_name_;
    const uint32_t partition_id_;

    std::string start_key_;
    std::string end_key_;
    std::string session_id_;
};

void FetchRecordArchivesCallback(void *data,
                                 ::google::protobuf::Closure *closure,
                                 DataStoreServiceClient &client,
                                 const remote::CommonResult &result);

struct FetchSnapshotArchiveCallbackData
{
    FetchSnapshotArchiveCallbackData(txservice::FetchSnapshotCc *fetch_cc,
                                     const std::string_view kv_table_name,
                                     uint32_t partition_id,
                                     std::string &&start_key,
                                     std::string &&end_key)
        : fetch_cc_(fetch_cc),
          kv_table_name_(kv_table_name),
          partition_id_(partition_id),
          start_key_(std::move(start_key)),
          end_key_(std::move(end_key)),
          session_id_("")
    {
    }

    txservice::FetchSnapshotCc *fetch_cc_;
    const std::string_view kv_table_name_;
    const uint32_t partition_id_;

    std::string start_key_;
    std::string end_key_;
    std::string session_id_;
};

void FetchSnapshotArchiveCallback(void *data,
                                  ::google::protobuf::Closure *closure,
                                  DataStoreServiceClient &client,
                                  const remote::CommonResult &result);

struct CreateSnapshotForBackupCallbackData : public SyncCallbackData,
                                             public Poolable
{
    CreateSnapshotForBackupCallbackData() = default;

    void Reset(std::string_view backup_name,
               uint64_t backup_ts,
               std::vector<std::string> *backup_files)
    {
        SyncCallbackData::Reset();
        backup_name_ = backup_name;
        backup_ts_ = backup_ts;
        backup_files_ = backup_files;
    }

    void Clear() override
    {
        backup_name_ = "";
        backup_ts_ = 0;
        backup_files_ = nullptr;
    }

    std::string_view backup_name_;
    uint64_t backup_ts_;
    std::vector<std::string> *backup_files_;
};

void CreateSnapshotForBackupCallback(void *data,
                                     ::google::protobuf::Closure *closure,
                                     DataStoreServiceClient &client,
                                     const remote::CommonResult &result);

}  // namespace EloqDS
