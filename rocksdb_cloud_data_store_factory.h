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
#include <string>

#include "data_store.h"
#include "data_store_factory.h"
#include "rocksdb_cloud_config.h"
#include "rocksdb_cloud_data_store.h"
#include "rocksdb_config.h"

#ifdef KV_DATA_STORE_TYPE_ROCKSDB

namespace EloqDS
{

class RocksDBCloudDataStoreFactory : public DataStoreFactory
{
public:
    RocksDBCloudDataStoreFactory(
        const ::EloqShare::RocksDBConfig &config,
#if ROCKSDB_CLOUD_FS()
        const ::EloqShare::RocksDBCloudConfig &cloud_config,
#endif
        const std::vector<TableName> &pre_built_tables,
        bool tx_enable_cache_replacement)
        : config_(config),
          cloud_config_(cloud_config),
          pre_built_tables_(pre_built_tables),
          tx_enable_cache_replacement_(tx_enable_cache_replacement)
    {
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true) override
    {
        auto ds = std::make_unique<RocksDBCloudDataStore>(
            cloud_config_,
            config_,
            create_if_missing,
            tx_enable_cache_replacement_,
            shard_id,
            data_store_service);
        for (const auto &table_name : pre_built_tables_)
        {
            ds->AppendPreBuiltTable(table_name);
        }
        ds->Connect();
        if (start_db)
        {
            bool ret = ds->StartDB();
            if (!ret)
            {
                LOG(ERROR)
                    << "Failed to start db instance in data store service";
                return nullptr;
            }
        }
        return ds;
    }

private:
    ::EloqShare::RocksDBConfig config_;
#if ROCKSDB_CLOUD_FS()
    ::EloqShare::RocksDBCloudConfig cloud_config_;
#endif
    std::vector<TableName> pre_built_tables_;
    bool tx_enable_cache_replacement_;
};

}  // namespace EloqDS

#endif
