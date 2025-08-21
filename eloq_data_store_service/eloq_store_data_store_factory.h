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
        store_config.data_page_restart_interval =
            eloq_store_configs_.data_page_restart_interval_;
        store_config.index_page_restart_interval =
            eloq_store_configs_.index_page_restart_interval_;
        store_config.init_page_count = eloq_store_configs_.init_page_count_;
        store_config.skip_verify_checksum =
            eloq_store_configs_.skip_verify_checksum_;
        store_config.index_buffer_pool_size =
            eloq_store_configs_.index_buffer_pool_size_;
        store_config.manifest_limit = eloq_store_configs_.manifest_limit_;
        store_config.io_queue_size = eloq_store_configs_.io_queue_size_;
        store_config.max_inflight_write =
            eloq_store_configs_.max_inflight_write_;
        store_config.max_write_batch_pages =
            eloq_store_configs_.max_write_batch_pages_;
        store_config.buf_ring_size = eloq_store_configs_.buf_ring_size_;
        store_config.coroutine_stack_size =
            eloq_store_configs_.coroutine_stack_size_;
        store_config.num_retained_archives =
            eloq_store_configs_.num_retained_archives_;
        store_config.archive_interval_secs =
            eloq_store_configs_.archive_interval_secs_;
        store_config.max_archive_tasks = eloq_store_configs_.max_archive_tasks_;
        store_config.file_amplify_factor =
            eloq_store_configs_.file_amplify_factor_;
        store_config.local_space_limit = eloq_store_configs_.local_space_limit_;
        store_config.reserve_space_ratio =
            eloq_store_configs_.reserve_space_ratio_;
        store_config.data_page_size = eloq_store_configs_.data_page_size_;
        store_config.pages_per_file_shift =
            eloq_store_configs_.pages_per_file_shift_;
        store_config.overflow_pointers = eloq_store_configs_.overflow_pointers_;
        store_config.data_append_mode = eloq_store_configs_.data_append_mode_;
        if (eloq_store_configs_.comparator_ != nullptr)
        {
            store_config.comparator_ = eloq_store_configs_.comparator_;
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
