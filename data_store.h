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

#include <string>
#include <unordered_map>
#include <vector>

#include "data_store_service_util.h"
#include "ds_request.pb.h"

namespace EloqDS
{

class DataStoreService;

class DataStore
{
public:
    // =======================================================
    // Group: External function interface called directly by DataStoreService
    // delegate to DataStoreServiceClient
    // =======================================================

    DataStore(uint32_t shard_id, DataStoreService *data_store_service)
        : shard_id_(shard_id), data_store_service_(data_store_service)
    {
    }

    virtual bool Connect() = 0;

    virtual bool StartDB(std::string cookie = "",
                         std::string prev_cookie = "") = 0;

    virtual void Shutdown() = 0;

    virtual void BatchFetchRecords(
        uint32_t req_shard_id,
        std::vector<::EloqDS::remote::FetchRecordsRequest::key> &&keys,
        std::function<
            void(std::vector<::EloqDS::remote::FetchRecordsResponse::record>
                     &&records,
                 const ::EloqDS::remote::CommonResult &result)> on_finish) = 0;

    /**
     * @brief flush entries in \@param batch to base table or skindex table in
     * data store, stop and return false if node_group is not longer leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual void BatchWriteRecords(
        uint32_t req_shard_id,
        std::vector<EloqDS::remote::BatchWriteRecordsRequest::record> &&batch,
        const std::string &kv_table_name,
        std::function<void(const ::EloqDS::remote::CommonResult &result)>
            on_finish) = 0;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param table_name base table name or sk index name
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual void CkptEnd(
        uint32_t req_shard_id,
        const std::string &table_name,
        std::function<void(const ::EloqDS::remote::CommonResult &result)>
            on_finish) = 0;

    /**
     * @brief Upsert table schema image to data store.
     * @param req_shard_id Requested shard id.
     * @param table_name Table name.
     * @param old_schema_img Old schema image.
     * @param new_schema_img New schema image.
     * @param op_type Operation type.
     * @param commit_ts Commit timestamp.
     * @param on_finish Callback function.
     */
    virtual void UpsertTable(
        uint32_t req_shard_id,
        const std::string table_name,
        const std::string old_schema_img,
        const std::string new_schema_img,
        ::EloqDS::remote::UpsertTableOperationType op_type,
        uint64_t commit_ts,
        std::function<void(const ::EloqDS::remote::CommonResult &result)>
            on_finish) = 0;

    /**
     * @brief Fetch table catalog from data store.
     * @param req_shard_id Requested shard id.
     * @param table_name Table name.
     * @param on_finish Callback function.
     */
    virtual void FetchTableCatalog(
        uint32_t req_shard_id,
        const std::string &table_name,
        std::function<void(std::string &&schema_image,
                           bool found,
                           uint64_t version_ts,
                           const ::EloqDS::remote::CommonResult &result)>
            on_finish) = 0;

    /**
     * @brief Switch the data store to read only mode.
     */
    virtual void SwitchToReadOnly() = 0;

    /**
     * @brief Switch the data store to read write mode.
     */
    virtual void SwitchToReadWrite() = 0;

    /**
     * @brief Open scan operation.
     * @param req_shard_id Requested shard id.
     * @param scan_req Scan request.
     * @param on_finish Callback function.
     */
    virtual void ScanOpen(
        const ::EloqDS::remote::ScanRequest &scan_req,
        std::function<void(const ::EloqDS::remote::ScanResponse &response)>
            on_finish) = 0;

    /**
     * @brief Fetch next scan result.
     * @param req_shard_id Requested shard id.
     * @param scan_req Scan request.
     * @param on_finish Callback function.
     */
    virtual void ScanNext(
        const ::EloqDS::remote::ScanRequest &scan_req,
        std::function<void(const ::EloqDS::remote::ScanResponse &response)>
            on_finish) = 0;

    /**
     * @brief Close scan operation.
     * @param req_shard_id Requested shard id.
     * @param session_id scan session id.
     * @param on_finish Callback function.
     */
    virtual void ScanClose(
        const ::EloqDS::remote::ScanRequest &scan_req,
        std::function<void(const ::EloqDS::remote::ScanResponse &response)>
            on_finish) = 0;

    /**
     * @brief Create snaphot of the current database file hierarchy.
     *        The snapshot files are stored in the backup_name.
     *        This operation need to be called after data store sevice switch to
     * read only.
     * @param backup_name The name of the backup.
     * @param snapshot_files The snapshot file list.
     */
    virtual void Backup(
        const uint32_t req_shard_id,
        const std::string &backup_name,
        std::function<void(const ::EloqDS::remote::CreateSnapshotResponse
                               &resp)> on_finish) = 0;

protected:
    uint32_t shard_id_;
    DataStoreService *data_store_service_;
};

}  // namespace EloqDS
