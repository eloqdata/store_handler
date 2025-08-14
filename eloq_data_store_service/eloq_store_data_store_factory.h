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

#include <memory>
#include <vector>

#include "data_store_factory.h"
#include "eloq_store_data_store.h"

namespace EloqDS
{
class EloqStoreDataStoreFactory : public DataStoreFactory
{
public:
    explicit EloqStoreDataStoreFactory(const EloqStoreConfig &configs)
        : eloq_store_configs_(configs)
    {
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true) override
    {
        ::eloqstore::KvOptions store_config;
        store_config.num_threads = eloq_store_configs_.worker_count_;
        store_config.store_path.emplace_back()
            .append(eloq_store_configs_.storage_path_)
            .append("/ds_")
            .append(std::to_string(shard_id));
        store_config.fd_limit = eloq_store_configs_.open_files_limit_;
        if (!eloq_store_configs_.cloud_store_path_.empty())
        {
            store_config.cloud_store_path
                .append(eloq_store_configs_.cloud_store_path_)
                .append("/ds_")
                .append(std::to_string(shard_id));
        }
        store_config.num_gc_threads = eloq_store_configs_.gc_threads_;
        store_config.rclone_threads = eloq_store_configs_.cloud_worker_count_;

        DLOG(INFO) << "Create EloqStore storage with workers: "
                   << store_config.num_threads
                   << ", store path: " << store_config.store_path.front()
                   << ", open files limit: " << store_config.fd_limit
                   << ", cloud store path: " << store_config.cloud_store_path
                   << ", gc threads: " << store_config.num_gc_threads
                   << ", cloud worker count: " << store_config.rclone_threads;
        auto ds = std::make_unique<EloqStoreDataStore>(
            shard_id, data_store_service, store_config);
        ds->Initialize();
        if (start_db)
        {
            ds->StartDB();
        }
        return ds;
    }

private:
    const EloqStoreConfig eloq_store_configs_;
};
}  // namespace EloqDS
