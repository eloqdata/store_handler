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
#include "cass_handler.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include "cc_map.h"
#include "cc_req_misc.h"
#include "data_store_handler.h"
#include "kv_store.h"
#include "local_cc_shards.h"
#include "metrics.h"
#include "partition.h"
#include "schema.h"

#ifndef ON_KEY_OBJECT
#include "sequences.h"
#endif

#include "bthread/timer_thread.h"
#include "cass/include/cassandra.h"
#include "cass_scanner.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service/include/cc/cc_entry.h"
#include "tx_service/include/cc/range_slice.h"
#include "tx_service/include/error_messages.h"
#include "tx_service/include/range_record.h"
#include "tx_service/include/type.h"

static const std::string cass_table_catalog_name = "table_catalogs";
static const std::string cass_database_catalog_name = "database_catalogs";
static const std::string cass_mvcc_archive_name = "mvcc_archives";
static const std::string cass_table_statistics_version_name =
    "table_statistics_version";
static const std::string cass_table_statistics_name = "table_statistics";
static const std::string cass_range_table_name = "table_ranges";
static const std::string cass_last_range_id_name =
    "table_last_range_partition_id";
static const std::string cass_cluster_config_name = "cluster_config";
static const std::string cass_eloq_kv_table_name = "eloq_kv_table";

static const std::unordered_set<std::string> cass_sys_tables(
    {cass_table_catalog_name,
     cass_database_catalog_name,
     cass_mvcc_archive_name,
     cass_table_statistics_version_name,
     cass_table_statistics_name,
     cass_range_table_name,
     cass_last_range_id_name,
     cass_cluster_config_name,
     cass_eloq_kv_table_name});

static const uint64_t future_wait_timeout = 10000000;
static thread_local std::unique_ptr<EloqDS::PartitionFinder> partition_finder;

EloqDS::CassHandler::CassHandler(const std::string &endpoint,
                                 const int port,
                                 const std::string &username,
                                 const std::string &password,
                                 const std::string &keyspace_name,
                                 const std::string &keyspace_class,
                                 const std::string &replicate_factor,
                                 bool high_compression_ratio,
                                 const int queue_size_io,
                                 bool bootstrap,
                                 bool ddl_skip_kv,
                                 uint32_t write_batch,
                                 uint32_t max_futures,
                                 uint32_t worker_pool_size)
    : write_batch_(write_batch),
      max_futures_(max_futures),
      prepared_cache_(),
      keyspace_name_(keyspace_name),
      keyspace_class_(keyspace_class),
      replicate_factor_(replicate_factor),
      high_compression_ratio_(high_compression_ratio),
      is_bootstrap_(bootstrap),
      ddl_skip_kv_(ddl_skip_kv),
      worker_pool_(worker_pool_size)
{
    cluster_ = cass_cluster_new();
    cass_cluster_set_contact_points(cluster_, endpoint.c_str());
    cass_cluster_set_port(cluster_, port);
    cass_cluster_set_credentials(cluster_, username.c_str(), password.c_str());
    cass_cluster_set_queue_size_io(cluster_, queue_size_io);
    cass_cluster_set_num_threads_io(cluster_, 10);
    session_ = cass_session_new();
}

EloqDS::CassHandler::~CassHandler()
{
    worker_pool_.Shutdown();

    prepared_cache_.clear();

    if (session_ != nullptr)
    {
        cass_session_free(session_);
    }

    if (cluster_ != nullptr)
    {
        cass_cluster_free(cluster_);
    }
}

/**
 * @brief
 * Each eloq cluster belongs to a keyspace in Cassandra.
 * Create and intialize the keyspace if it doesn't exists.
 *
 * On Cassandra handler initialization, we will firstly try to connect to
 * target keyspace directly, and skip InitializeKeySpace() if connect succeeds.
 */
bool EloqDS::CassHandler::InitializeKeySpace()
{
    CassError rc = CASS_OK;

    // create keyspace if not exists in Cassandra.
    CassStatement *create_statement = NULL;
    CassFuture *create_future = NULL;

    std::string ck_query = "CREATE KEYSPACE ";
    ck_query.append(keyspace_name_);
    ck_query.append(" WITH REPLICATION = {'class' : '");
    ck_query.append(keyspace_class_);
    ck_query.append("', 'replication_factor' : ");
    ck_query.append(replicate_factor_);
    ck_query.append("}");

    create_statement = cass_statement_new(ck_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        if (rc == CASS_ERROR_SERVER_ALREADY_EXISTS)
        {
            return true;
        }
        return false;
    }

    // create mvcc archvies table in keyspace to store historical versions.
    if (!CreateMvccArchivesTable())
    {
        return false;
    }

    // create table_catalogs in keyspace to store table catalog.
    std::string catalog_table_name;
    catalog_table_name.reserve(keyspace_name_.size() +
                               cass_table_catalog_name.size() + 1);
    catalog_table_name.append(keyspace_name_);
    catalog_table_name.append(".");
    catalog_table_name.append(cass_table_catalog_name);
    std::string ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(catalog_table_name);
    ct_query.append(
        "(tablename text primary key, content blob, kvtablename "
        "text, kvindexname text, keyschemasts text, version bigint)");

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

    // create `databases_catalog` in keyspace to store database catalog.
    catalog_table_name.clear();
    catalog_table_name.reserve(keyspace_name_.size() +
                               cass_database_catalog_name.size() + 1);
    catalog_table_name.append(keyspace_name_);
    catalog_table_name.append(".");
    catalog_table_name.append(cass_database_catalog_name);
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(catalog_table_name);
    ct_query.append(
        "(dbname text primary key, definition blob, cataloginfo blob)");

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

    // Create Physical Kv Table
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(keyspace_name_);
    ct_query.append(".");
    ct_query.append(cass_eloq_kv_table_name);
    ct_query.append(" (");
    ct_query.append(
        "\"___mono_key___\" blob, \"___unpack_info___\" blob, "
        "\"___encoded_blob___\" blob, \"___version___\" bigint, "
        "\"___deleted___\" boolean, "
        "kvtablename text, pk1_ int, pk2_ smallint, primary "
        "key((kvtablename, pk1_, pk2_), "
        "\"___mono_key___\")) WITH gc_grace_seconds = 86401 ");  // one day plus
                                                                 // one second
    if (high_compression_ratio_)
    {
        ct_query.append(
            " AND compression={'class': "
            "'org.apache.cassandra.io.compress.ZstdCompressor'}");
    }

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

#ifdef RANGE_PARTITION_ENABLED
    // Create last_range_partition_id table
    std::string range_partition_table_name;
    range_partition_table_name.reserve(keyspace_name_.size() +
                                       cass_last_range_id_name.size() + 1);
    range_partition_table_name.append(keyspace_name_);
    range_partition_table_name.append(".");
    range_partition_table_name.append(cass_last_range_id_name);
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(range_partition_table_name);
    ct_query.append("(tablename text primary key, last_partition_id int)");

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

    // Create table_ranges table
    std::string ranges_table_name;
    ranges_table_name.reserve(keyspace_name_.size() +
                              cass_range_table_name.size() + 1);
    ranges_table_name.append(keyspace_name_);
    ranges_table_name.append(".");
    ranges_table_name.append(cass_range_table_name);
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(ranges_table_name);
    ct_query.append("( ");
    ct_query.append("    tablename text,");
    ct_query.append("  \"___mono_key___\" blob,");
    ct_query.append("  \"___segment_id___\" bigint,");
    ct_query.append("  \"___segment_cnt___\" bigint,");
    ct_query.append("  \"___partition_id___\" int,");
    ct_query.append("  \"___version___\" bigint,");
    ct_query.append("  \"___slice_keys___\" blob,");
    ct_query.append("  \"___slice_sizes___\" blob,");
    ct_query.append("  \"___slice_version___\" bigint,");
    ct_query.append(
        "  PRIMARY KEY(tablename, \"___mono_key___\", \"___segment_id___\")");
    ct_query.append(" )");

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }
#endif

    // Create table_statistics_version
    std::string table_statistics_version;
    table_statistics_version.reserve(
        keyspace_name_.size() + cass_table_statistics_version_name.size() + 1);
    table_statistics_version.append(keyspace_name_);
    table_statistics_version.append(".");
    table_statistics_version.append(cass_table_statistics_version_name);
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(table_statistics_version);
    ct_query.append("(");
    ct_query.append("tablename text, ");
    ct_query.append("version bigint, ");
    ct_query.append("PRIMARY KEY(tablename)");
    ct_query.append(")");
    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

    // Create table_statistics table
    std::string table_statistics;
    table_statistics.reserve(keyspace_name_.size() +
                             cass_table_statistics_name.size() + 1);
    table_statistics.append(keyspace_name_);
    table_statistics.append(".");
    table_statistics.append(cass_table_statistics_name);
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(table_statistics);
    ct_query.append("(");
    ct_query.append("tablename text, ");
    ct_query.append("version bigint, ");
    ct_query.append("indextype tinyint, ");
    ct_query.append("indexname text, ");
    ct_query.append("segment_id int, ");
    ct_query.append("records bigint, ");
    ct_query.append("samplekeys set<blob>, ");
    ct_query.append(
        "PRIMARY KEY(tablename, version, indextype, indexname, segment_id)");
    ct_query.append(")");

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

    // Create cluster_config table
    std::string cluster_config;
    cluster_config.reserve(keyspace_name_.size() +
                           cass_cluster_config_name.size() + 1);
    cluster_config.append(keyspace_name_);
    cluster_config.append(".");
    cluster_config.append(cass_cluster_config_name);
    ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(cluster_config);
    ct_query.append("(");
    ct_query.append("pk int, ");
    ct_query.append("ngids list<int>, ");
    ct_query.append("ips list<text>, ");
    ct_query.append("ports list<smallint>, ");
    ct_query.append("ng_members list<text>, ");
    ct_query.append("version bigint, ");
    ct_query.append("node_ids list<int>, ");
    ct_query.append("ng_members_is_candidate list<text>, ");
    ct_query.append("PRIMARY KEY(pk)");  // dummy pk column
    ct_query.append(")");

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_statement_free(create_statement);
        cass_future_free(create_future);
        return false;
    }
    rc = cass_future_error_code(create_future);

    cass_statement_free(create_statement);
    cass_future_free(create_future);
    if (rc != CASS_OK)
    {
        return false;
    }

    if (!InitPreBuiltTables())
    {
        return false;
    }

    return rc == CASS_OK;
}

bool EloqDS::CassHandler::Connect()
{
    bool succeed = false;
    for (int retry = 1; retry <= 5 && !succeed; retry++)
    {
        /* Provide the cluster object as configuration to connect the session */
        CassFuture *connect_future = cass_session_connect(session_, cluster_);

        /* This operation will block until the result is ready */
        CassError rc = cass_future_error_code(connect_future);
        cass_future_free(connect_future);
        if (rc == CASS_OK)
        {
            // connect succeeds;
            //
            // keyspace may not initialized yet, initialize it. Note that
            // InitializeKeySpace() is safe to be called by other runtimes
            // concurrently.
            if (InitializeKeySpace())
            {
                succeed = true;
            }
            else
            {
                cass_session_close(session_);
                if (retry == 5)
                {
                    LOG(ERROR)
                        << "Cassandra error: failed to initialize keyspace:"
                        << keyspace_name_;
                }
            }
        }
        else if (rc != CASS_OK)
        {
            if (retry == 5)
            {
                LOG(ERROR) << "Cassandra connection error: "
                           << cass_error_desc(rc);
            }
            else
            {
                LOG(WARNING) << "Cassandra connection fail, wait for next "
                                "retry after 100 ms";
                // Sleep for 100 milliseconds
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    if (succeed)
    {
        ScheduleTimerTasks();
    }
    return succeed;
}

void EloqDS::CassHandler::ScheduleTimerTasks()
{
    /*
    timer_thd_.start(nullptr);
    CleanDefunctKvTables(this);
    */
}

bool EloqDS::CassHandler::PutAllExecute(
    const txservice::TableName &table_name,
    const CassPrepared *insert_prepared,
    const CassPrepared *insert_ttl_prepared,
    const CassPrepared *delete_prepared,
    std::vector<txservice::FlushRecord> &batch,
    const TableSchema *table_schema,
    uint32_t node_group)
{
    size_t flush_idx = 0;
    CassBatchExecutor cass_batch(session_);
    cass_batch.flush_table_type_ = CassBatchExecutor::FlushTableType::Base;

#ifndef RANGE_PARTITION_ENABLED
    std::sort(
        batch.begin(),
        batch.end(),
        [](const txservice::FlushRecord &lhs, const txservice::FlushRecord &rhs)
        { return lhs.partition_id_ < rhs.partition_id_; });
#endif

    int32_t last_pk = -1;

    while (flush_idx < batch.size() &&
           Sharder::Instance().LeaderTerm(node_group) > 0)
    {
        for (; flush_idx < batch.size() &&
               cass_batch.PendingFutureCount() < max_futures_;
             ++flush_idx)
        {
            using namespace txservice;
            FlushRecord &ckpt_rec = batch.at(flush_idx);

            if (last_pk == -1)
            {
                last_pk = ckpt_rec.partition_id_;
            }

            if (last_pk != ckpt_rec.partition_id_)
            {
                last_pk = ckpt_rec.partition_id_;
                break;
            }

            uint32_t tuple_size = 0;

            CassStatement *statement =
                PutAllCreateStatement(table_name,
                                      insert_prepared,
                                      insert_ttl_prepared,
                                      delete_prepared,
                                      ckpt_rec,
                                      table_schema,
                                      ckpt_rec.partition_id_,
                                      -1);
            tuple_size += ckpt_rec.Key().Size();
            if (ckpt_rec.Payload() != nullptr)
            {
                const TxRecord *payload = ckpt_rec.Payload();
                tuple_size += payload->Length();
            }
            CassError rc = cass_batch.AddBatchStatement(statement, tuple_size);
            if (rc != CASS_OK)
            {
                LOG(WARNING) << "CassHandler: Failed to add batch.";
                return false;
            }
        }

        // Send out the last batch since we've just moved to a new partition.
        cass_batch.Execute();

        // Wait for future result for every max_futures_.
        if (cass_batch.PendingFutureCount() >= max_futures_ ||
            flush_idx == batch.size())
        {
            uint retry = 0;
            CassError ce = cass_batch.Wait();
            while (retry < 5 && ce != CASS_OK)
            {
                std::this_thread::sleep_for(std::chrono::seconds(retry * 2));
                retry++;
                ce = cass_batch.Retry();
            }
            if (ce != CASS_OK)
            {
                LOG(WARNING) << "CassHandler: Failed to execute batch.";
                return false;
            }
        }
    }

    if (Sharder::Instance().LeaderTerm(node_group) < 0)
    {
        LOG(WARNING) << "CassHandler: leader transferred of ng#" << node_group;
        return false;
    }
    if (flush_idx < batch.size())
    {
        LOG(ERROR)
            << "CassHandler: flushed records count mismatch, target count: "
            << batch.size() << ", and the actual count: " << flush_idx;
        return false;
    }
    return true;
}

const CassPrepared *EloqDS::CassHandler::GetInsertPrepared(
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema,
    bool with_ttl)
{
    const CassPrepared *insert_prepared = nullptr;
    if (with_ttl)
    {
        insert_prepared = GetCachedPreparedStmt(
            cass_eloq_kv_table_name, 0, CassPreparedType::InsertTTL);
    }
    else
    {
        insert_prepared = GetCachedPreparedStmt(
            cass_eloq_kv_table_name, 0, CassPreparedType::Insert);
    }

    if (insert_prepared == nullptr)
    {
        std::string insert_str("INSERT INTO ");
        insert_str.append(keyspace_name_);
        insert_str.append(".");
        insert_str.append(cass_eloq_kv_table_name);
        insert_str.append(" (");

        uint16_t record_col_cnt = 0;

        insert_str.append(" \"___encoded_blob___\", ");
        record_col_cnt += 1;

        // Pseudo columns for cass partitioning
        insert_str.append(
            " \"___mono_key___\", \"___unpack_info___\", \"___version___\", "
            "\"___deleted___\", pk1_, pk2_, kvtablename) VALUES (");

        for (uint16_t idx = 0; idx < record_col_cnt + 7; ++idx)
        {
            insert_str.append("?,");
        }
        insert_str.erase(insert_str.size() - 1);
        insert_str.append(") USING TIMESTAMP ?");
        if (with_ttl)
        {
            insert_str.append(" AND TTL ?");
        }

        CassFuture *future = cass_session_prepare(session_, insert_str.c_str());
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            return nullptr;
        }

        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
        {
            LOG(ERROR) << "PutAll Insert SQL Prepare Error: "
                       << ErrorMessage(future) << " SQL: " << insert_str;
            cass_future_free(future);
        }
        else
        {
            insert_prepared = cass_future_get_prepared(future);
            if (with_ttl)
            {
                insert_prepared =
                    CachePreparedStmt(cass_eloq_kv_table_name,
                                      0,
                                      insert_prepared,
                                      CassPreparedType::InsertTTL);
            }
            else
            {
                insert_prepared = CachePreparedStmt(cass_eloq_kv_table_name,
                                                    0,
                                                    insert_prepared,
                                                    CassPreparedType::Insert);
            }
            cass_future_free(future);
        }
    }
    return insert_prepared;
}

const CassPrepared *EloqDS::CassHandler::GetDeletePrepared(
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema)
{
    const CassPrepared *delete_prepared = GetCachedPreparedStmt(
        cass_eloq_kv_table_name, 0, CassPreparedType::Delete);
    // TODO(lokax): redis. Delete data from

    if (delete_prepared == nullptr)
    {
        // Using "___deleted___" column to specifies the key whether is deleted.
        // The cql statement used for insert and delete operations are similar.
        // The differences are:  delete operation statement binds "null" for
        // NonPkColumn and "___unpack_info___" column, binds "cass_true" for
        // "___deleted___" column. Deleted key only live 24 hours in kvstore.

        std::string del_str("INSERT INTO ");
        del_str.append(keyspace_name_);
        del_str.append(".");
        del_str.append(cass_eloq_kv_table_name);
        del_str.append(" (");

        uint16_t record_col_cnt = 0;

        del_str.append(" \"___encoded_blob___\", ");
        record_col_cnt += 1;

        // Pseudo columns for cass partitioning
        del_str.append(
            " \"___unpack_info___\", \"___mono_key___\", \"___version___\", "
            "\"___deleted___\", pk1_, pk2_, kvtablename) VALUES (");

        for (uint16_t idx = 0; idx < record_col_cnt + 7; ++idx)
        {
            del_str.append("?,");
        }
        del_str.erase(del_str.size() - 1);
        del_str.append(") USING TIMESTAMP ? AND TTL 86400");

        CassFuture *future = cass_session_prepare(session_, del_str.c_str());
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            return nullptr;
        }

        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
        {
            LOG(ERROR) << "PutAll Delete SQL Prepare Error: "
                       << ErrorMessage(future) << " SQL: " << del_str;
            cass_future_free(future);
        }
        else
        {
            delete_prepared = cass_future_get_prepared(future);
            delete_prepared = CachePreparedStmt(cass_eloq_kv_table_name,
                                                0,
                                                delete_prepared,
                                                CassPreparedType::Delete);
            cass_future_free(future);
        }
    }
    return delete_prepared;
}

const CassPrepared *EloqDS::CassHandler::GetReadPrepared(
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema)
{
    const CassPrepared *read_prepared = GetCachedPreparedStmt(
        cass_eloq_kv_table_name, 0, CassPreparedType::Read);

    if (read_prepared == nullptr)
    {
        std::string read_str("SELECT ");
        read_str.append(" \"___encoded_blob___\", ");
        read_str.append(
            " \"___mono_key___\", \"___unpack_info___\", \"___version___\", "
            "\"___deleted___\" FROM ");
        read_str.append(keyspace_name_);
        read_str.append(".");
        read_str.append(cass_eloq_kv_table_name);
        read_str.append(
            " WHERE kvtablename =? AND pk1_=? AND pk2_=? AND "
            "\"___mono_key___\"=?");

        CassFuture *future = cass_session_prepare(session_, read_str.c_str());
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            return nullptr;
        }

        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
        {
            LOG(ERROR) << ErrorMessage(future);
            cass_future_free(future);
        }
        else
        {
            read_prepared = cass_future_get_prepared(future);
            read_prepared = CachePreparedStmt(cass_eloq_kv_table_name,
                                              0,
                                              read_prepared,
                                              CassPreparedType::Read);
            cass_future_free(future);
        }
    }
    return read_prepared;
}

CassStatement *EloqDS::CassHandler::PutAllCreateStatement(
    const txservice::TableName &table_name,
    const CassPrepared *insert_prepared,
    const CassPrepared *insert_ttl_prepared,
    const CassPrepared *delete_prepared,
    const txservice::FlushRecord &ckpt_rec,
    const TableSchema *table_schema,
    int32_t pk1,
    int16_t pk2)
{
    txservice::TxKey tx_key = ckpt_rec.Key();
    const txservice::TxRecord *tx_record = ckpt_rec.Payload();

    const std::string &physical_table_name =
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);

    CassStatement *statement = nullptr;
    assert(ckpt_rec.payload_status_ == RecordStatus::Normal ||
           ckpt_rec.payload_status_ == RecordStatus::Deleted);
    uint64_t now = LocalCcShards::ClockTsInMillseconds();

    if (ckpt_rec.payload_status_ != RecordStatus::Deleted &&
        (!tx_record->HasTTL() || tx_record->GetTTL() > now))
    {
        if (tx_record->HasTTL())
        {
            // Upserts a key to the k-v store
            statement = cass_prepared_bind(insert_ttl_prepared);
        }
        else
        {
            statement = cass_prepared_bind(insert_prepared);
        }
        cass_statement_set_is_idempotent(statement, cass_true);

        if (table_name.Type() == txservice::TableType::Primary ||
            table_name.Type() == txservice::TableType::UniqueSecondary)
        {
            // Bind ___encoded_blob___
            cass_statement_bind_bytes(
                statement,
                0,
                reinterpret_cast<const uint8_t *>(tx_record->EncodedBlobData()),
                tx_record->EncodedBlobSize());
        }
        else
        {
            assert(table_name.Type() == txservice::TableType::Secondary);
            cass_statement_bind_null(statement, 0);
        }

        size_t cc = 1;
        // Binds packed key value.
        cass_statement_bind_bytes(
            statement,
            cc,
            reinterpret_cast<const uint8_t *>(tx_key.Data()),
            tx_key.Size());

        // Bind unpack info
        if (tx_record->UnpackInfoSize() > 0)
        {
            cass_statement_bind_bytes(
                statement,
                cc + 1,
                reinterpret_cast<const uint8_t *>(tx_record->UnpackInfoData()),
                tx_record->UnpackInfoSize());
        }
        else
        {
            cass_statement_bind_bytes(statement, cc + 1, nullptr, 0);
        }

        // Binds the record's "___version___" column.
        cass_statement_bind_int64(statement, cc + 2, ckpt_rec.commit_ts_);

        // Binds the record's "___deleted__" column.
        cass_statement_bind_bool(statement, cc + 3, cass_false);

        // Binds the partition ID.
        cass_statement_bind_int32(statement, cc + 4, pk1);
        cass_statement_bind_int16(statement, cc + 5, pk2);
        // Bind kv table name prefix
        cass_statement_bind_string(
            statement, cc + 6, physical_table_name.data());

        // Binds the timestamp of the USING TIMESTAMP clause.
        cass_statement_bind_int64(statement, cc + 7, ckpt_rec.commit_ts_);

        // bind ttl if it has
        if (tx_record->HasTTL())
        {
            int64_t ttl_gap =
                tx_record->GetTTL() - LocalCcShards::ClockTsInMillseconds();
            ttl_gap = ttl_gap < 0 ? 0 : ttl_gap;
            uint64_t ttl = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::milliseconds(ttl_gap))
                               .count();
            // ttl must be bind using int32
            cass_statement_bind_int32(statement, cc + 8, ttl);
        }
    }
    else
    {
        // Deletes a key from the k-v store through setting the
        // "___deleted___" column as true.

        statement = cass_prepared_bind(delete_prepared);
        cass_statement_set_is_idempotent(statement, cass_true);

        size_t cc = 2;  // includesd encoded_blob and unpack_info
        // "encoded_blob" and "unpack_info column" bind null
        for (size_t i = 0; i < cc; i++)
        {
            cass_statement_bind_null(statement, i);
        }

        // Binds tx key.
        cass_statement_bind_bytes(
            statement,
            cc,
            reinterpret_cast<const uint8_t *>(tx_key.Data()),
            tx_key.Size());

        // Binds the record's "___version___" column.
        cass_statement_bind_int64(statement, cc + 1, ckpt_rec.commit_ts_);

        // Binds the record's "___deleted__" column.
        cass_statement_bind_bool(statement, cc + 2, cass_true);

        // Binds the partition ID.
        cass_statement_bind_int32(statement, cc + 3, pk1);
        cass_statement_bind_int16(statement, cc + 4, pk2);
        // Bind table uuid
        cass_statement_bind_string(
            statement, cc + 5, physical_table_name.data());

        // Binds the timestamp of the USING TIMESTAMP clause.
        cass_statement_bind_int64(statement, cc + 6, ckpt_rec.commit_ts_);
    }

    return statement;
}

bool EloqDS::CassHandler::PutAll(std::vector<txservice::FlushRecord> &batch,
                                 const txservice::TableName &table_name,
                                 const txservice::TableSchema *table_schema,
                                 uint32_t node_group)
{
    LOG(INFO) << "PutAll begin, table:" << table_name.String()
              << ", batch size:" << batch.size();
    uint64_t begin_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();

    const CassPrepared *insert_prepared =
        GetInsertPrepared(table_name, table_schema);
    const CassPrepared *insert_ttl_prepared =
        GetInsertPrepared(table_name, table_schema, true);
    const CassPrepared *delete_prepared =
        GetDeletePrepared(table_name, table_schema);

    if (insert_prepared == nullptr or delete_prepared == nullptr)
    {
        return false;
    }

    bool res = PutAllExecute(table_name,
                             insert_prepared,
                             insert_ttl_prepared,
                             delete_prepared,
                             batch,
                             table_schema,
                             node_group);

    uint64_t end_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    LOG(INFO) << "PutAll end, table:" << table_name.String()
              << ", result:" << (int) (res) << ", batch size:" << batch.size()
              << ", used time(ms):" << (end_ts - begin_ts);

    return res;
}

void EloqDS::CassHandler::UpsertTable(
    const txservice::TableSchema *old_table_schema,
    const txservice::TableSchema *table_schema,
    txservice::OperationType op_type,
    uint64_t write_time,
    txservice::NodeGroupId ng_id,
    int64_t tx_term,
    txservice::CcHandlerResult<txservice::Void> *hd_res,
    const txservice::AlterTableInfo *alter_table_info,
    txservice::CcRequestBase *cc_req,
    txservice::CcShard *ccs,
    txservice::CcErrorCode *err_code)
{
    int64_t leader_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (leader_term < 0)
    {
        hd_res->SetError(CcErrorCode::TX_NODE_NOT_LEADER);
        return;
    }

    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *) { Sharder::Instance().UnpinNodeGroupData(ng_id); });

    if (leader_term != tx_term)
    {
        hd_res->SetError(CcErrorCode::NG_TERM_CHANGED);
        return;
    }

    // Use old schema for drop table as the new schema would be null.
    const txservice::TableSchema *schema =
        (op_type == OperationType::DropTable ||
         op_type == OperationType::TruncateTable)
            ? old_table_schema
            : table_schema;
    const CassCatalogInfo *cass_info =
        static_cast<const CassCatalogInfo *>(schema->GetKVCatalogInfo());
    (void) cass_info;

    UpsertTableData *table_data =
        new UpsertTableData(this,
                            ng_id,
                            &schema->GetBaseTableName(),
                            old_table_schema,
                            table_schema,
                            op_type,
                            session_,
                            write_time,
                            is_bootstrap_,
                            ddl_skip_kv_,
                            high_compression_ratio_,
                            defer_unpin,
                            hd_res,
                            tx_service_,
                            alter_table_info);

    // Upsert PK(base) table.
    switch (op_type)
    {
    case OperationType::DropTable:
    {
        DeleteDataFromKvTable(&schema->GetBaseTableName(), table_data);
        break;
    }
    case OperationType::TruncateTable:
    {
        DeleteDataFromKvTable(&schema->GetBaseTableName(), table_data);
        break;
    }
    case OperationType::CreateTable:
    {
        if (ddl_skip_kv_ && !is_bootstrap_)
        {
            // skip create/drop table on kv store to speed up test case only.
            OnUpsertCassTable(nullptr, table_data);
        }
        else
        {
            // Fill ./mysql/sequences table schema for Sequences::instance_
            // At this point the schema operation of ./mysql/sequences is
            // irrevertible and the schema will not be changed so we can safely
            // install the schema here.

#ifndef ON_KEY_OBJECT
            if (schema->GetBaseTableName() == Sequences::table_name_)
            {
                Sequences::SetTableSchema(schema);
            }
#else
            assert(false);
#endif

            OnUpsertCassTable(nullptr, table_data);
        }
        break;
    }
    case OperationType::Update:
    case OperationType::AddIndex:
    case OperationType::DropIndex:
        // Skip base table update
        break;
    default:
        LOG(ERROR) << "Unsupported command for CassHandler::UpsertTable.";
        break;
    }

    // Upsert SK tables.
    if (op_type == OperationType::AddIndex ||
        op_type == OperationType::DropIndex ||
        (op_type != OperationType::Update && schema->IndexesSize() > 0))
    {
        table_data->RewindSKTableIteratorMarkFirstForUpserting();
        while (!table_data->IsSKTableIteratorEnd())
        {
            UpsertSkTable(table_data);
            table_data->MarkNextSKTableForUpserting();
        }
    }

    // Check if we are the last referencer of table data.
    if (table_data->ref_count_.fetch_sub(1) == 1)
    {
        CcErrorCode error_code = table_data->ErrorCode();
        if (error_code != txservice::CcErrorCode::NO_ERROR)
        {
            table_data->hd_res_->SetError(error_code);
            // If one of the previous upsert table failed, clean up memory and
            // leave.
            delete table_data;
        }
        else
        {
            // Done with all cass table upsert, update the mariadb_tables
            // record.
            UpsertCatalog(table_data);
        }
    }
}

void EloqDS::CassHandler::OnUpsertCassTable(CassFuture *future, void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    CassError code = CASS_OK;
    if (future)
    {
        code = cass_future_error_code(future);
    }
    if (code != CASS_OK)
    {
        LOG(ERROR) << ErrorMessage(future)
                   << " table name: " << table_data->table_name_->String();
        table_data->SetErrorCode(CcErrorCode::DATA_STORE_ERR);
    }

    if (table_data->ref_count_.fetch_sub(1) == 1)
    {
        CcErrorCode error_code = table_data->ErrorCode();
        if (error_code != CcErrorCode::NO_ERROR)
        {
            table_data->hd_res_->SetError(error_code);
            // If one of the previous upsert table failed, clean up memory and
            // leave.
            delete table_data;
        }
        else
        {
            // Done with all cass table upsert, update the mariadb_tables
            // record.
            UpsertCatalog(table_data);
        }
    }
}

void EloqDS::CassHandler::UpsertSkTable(UpsertTableData *table_data)
{
    const txservice::TableName *table_name =
        table_data->GetMarkedUpsertingTableName();

    switch (table_data->op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::AddIndex:
    {
        if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
        {
            OnUpsertCassTable(nullptr, table_data);
        }
        else
        {
            OnUpsertCassTable(nullptr, table_data);
        }
        break;
    }
    case OperationType::DropTable:
    case OperationType::DropIndex:
    {
        // in case one transaction create->drop->create table(2nd create with
        // different schema)
        // Now, we only use `cass_eloq_kv_table` as kv_table, so we don't need
        // to erase cache

        table_data->cass_hd_->DeleteDataFromKvTable(table_name, table_data);

        break;
    }
    default:
        LOG(INFO) << "Unsupported command for CassHandler::UpsertSkTable.";
        break;
    }
}

void EloqDS::CassHandler::UpsertCatalog(UpsertTableData *table_data)
{
    CassStatement *statement = nullptr;
    CassFuture *st_future = nullptr;

    switch (table_data->op_type_)
    {
    case OperationType::DropTable:
    {
        std::string table_key = table_data->table_name_->Serialize();
        std::string delete_str("DELETE FROM ");
        delete_str.append(table_data->cass_hd_->keyspace_name_);
        delete_str.append(".");
        delete_str.append(cass_table_catalog_name);
        delete_str.append(" USING TIMESTAMP ");
        delete_str.append(std::to_string(table_data->write_time_));
        delete_str.append(" WHERE tablename ='");
        delete_str.append(table_key);
        delete_str.append("'");
        statement = cass_statement_new(delete_str.c_str(), 0);
        st_future = cass_session_execute(table_data->session_, statement);
        break;
    }
    case OperationType::TruncateTable:
    {
        std::string table_key = table_data->table_name_->Serialize();

        std::string update_str("UPDATE ");
        update_str.append(table_data->cass_hd_->keyspace_name_);
        update_str.append(".");
        update_str.append(cass_table_catalog_name);
        update_str.append(" SET version =");
        update_str.append(
            std::to_string(table_data->new_table_schema_->Version()));
        update_str.append(", kvtablename ='");
        update_str.append(
            table_data->new_table_schema_->GetKVCatalogInfo()->kv_table_name_);
        update_str.append("' WHERE tablename ='");
        update_str.append(table_key);
        update_str.append("'");

        statement = cass_statement_new(update_str.c_str(), 0);
        st_future = cass_session_execute(table_data->session_, statement);
        break;
    }
    case OperationType::CreateTable:
    case OperationType::Update:
    case OperationType::AddIndex:
    case OperationType::DropIndex:
    {
        const CassCatalogInfo *cass_info = static_cast<const CassCatalogInfo *>(
            table_data->schema_->GetKVCatalogInfo());
        std::string table_key = table_data->table_name_->Serialize();
        // create table catalog information stored in table_catalogs
        // Inserts an entire row or upserts data into an existing row,
        // using the full primary key
        std::string insert_str("INSERT INTO ");
        insert_str.append(table_data->cass_hd_->keyspace_name_);
        insert_str.append(".");
        insert_str.append(cass_table_catalog_name);
        insert_str.append(
            " (tablename, content, kvtablename, kvindexname, "
            "keyschemasts, version)"
            " VALUES ('");
        insert_str.append(table_key);
        insert_str.append("', 0x");

        const std::string &catalog_image = table_data->schema_->SchemaImage();
        std::string frm, kv_info, key_schemas_ts_str;
        DeserializeSchemaImage(catalog_image, frm, kv_info, key_schemas_ts_str);

        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (size_t pos = 0; pos < frm.length(); ++pos)
        {
            ss << std::setw(2)
               << static_cast<unsigned>(static_cast<uint8_t>(frm.at(pos)));
        }
        // serialize kvtablename
        insert_str.append(ss.str());
        insert_str.append(", '");
        insert_str.append(cass_info->kv_table_name_);
        insert_str.append("', '");

        auto *table_schema = table_data->schema_;
        std::string key_schemas_ts;
        key_schemas_ts
            .append(std::to_string(table_schema->KeySchema()->SchemaTs()))
            .append(" ");

        // serialize kvindexname
        std::string index_names;
        if (cass_info->kv_index_names_.size() != 0)
        {
            for (auto it = cass_info->kv_index_names_.cbegin();
                 it != cass_info->kv_index_names_.cend();
                 ++it)
            {
                index_names.append(it->first.StringView())
                    .append(" ")
                    .append(it->second)
                    .append(" ")
                    .append(1, static_cast<char>(it->first.Engine()))
                    .append(" ");

                auto *sk_schema = table_schema->IndexKeySchema(it->first);
                key_schemas_ts.append(it->first.StringView())
                    .append(" ")
                    .append(std::to_string(sk_schema->SchemaTs()))
                    .append(" ");
            }
            index_names.pop_back();
        }
        else
        {
            index_names.clear();
        }
        key_schemas_ts.pop_back();

        insert_str.append(index_names);

        insert_str.append("', '");
        insert_str.append(key_schemas_ts);

        insert_str.append("', ");
        insert_str.append(std::to_string(table_schema->Version()));
        insert_str.append(") USING TIMESTAMP ");
        insert_str.append(std::to_string(table_data->write_time_));
        statement = cass_statement_new(insert_str.c_str(), 0);
        st_future = cass_session_execute(table_data->session_, statement);
        break;
    }
    default:
        LOG(ERROR) << "Unsupported command for CassHandler::UpsertCatalog";
        break;
    }

    cass_future_set_callback(st_future, OnUpsertCatalog, table_data);

    cass_future_free(st_future);
    cass_statement_free(statement);
}

void EloqDS::CassHandler::OnUpsertCatalog(CassFuture *future, void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    CassError code = cass_future_error_code(future);
    if (code != CASS_OK)
    {
        LOG(ERROR) << ErrorMessage(future);
        table_data->hd_res_->SetError(CcErrorCode::DATA_STORE_ERR);
        delete table_data;
        return;
    }

    switch (table_data->op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::DropTable:
    case OperationType::AddIndex:
    case OperationType::DropIndex:
        UpsertTableStatistics(table_data);
        break;
    case OperationType::TruncateTable:
        table_data->hd_res_->SetFinished();
        delete table_data;
        break;
    case OperationType::Update:
        table_data->hd_res_->SetFinished();
        delete table_data;
        break;
    default:
        assert(false);
        break;
    }
}

void EloqDS::CassHandler::UpsertTableStatistics(UpsertTableData *table_data)
{
    if (table_data->op_type_ == OperationType::DropTable)
    {
        std::string table_key = table_data->table_name_->Serialize();

        std::string delete_str_1("DELETE FROM ");
        delete_str_1.append(table_data->cass_hd_->keyspace_name_);
        delete_str_1.append(".");
        delete_str_1.append(cass_table_statistics_name);
        delete_str_1.append(" WHERE tablename=?");

        std::string delete_str_2("DELETE FROM ");
        delete_str_2.append(table_data->cass_hd_->keyspace_name_);
        delete_str_2.append(".");
        delete_str_2.append(cass_table_statistics_version_name);
        delete_str_2.append(" WHERE tablename=?");

        CassStatement *delete_stmt_1 =
            cass_statement_new(delete_str_1.c_str(), 1);
        CassStatement *delete_stmt_2 =
            cass_statement_new(delete_str_2.c_str(), 1);

        cass_statement_bind_string_n(
            delete_stmt_1, 0, table_key.data(), table_key.size());
        cass_statement_bind_string_n(
            delete_stmt_2, 0, table_key.data(), table_key.size());

        CassBatch *delete_batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        cass_batch_add_statement(delete_batch, delete_stmt_1);
        cass_batch_add_statement(delete_batch, delete_stmt_2);

        CassFuture *delete_future =
            cass_session_execute_batch(table_data->session_, delete_batch);
        cass_future_set_callback(
            delete_future, OnUpsertTableStatistics, table_data);

        cass_future_free(delete_future);
        cass_batch_free(delete_batch);
        cass_statement_free(delete_stmt_2);
        cass_statement_free(delete_stmt_1);
    }
    else
    {
        // create or alter table
        OnUpsertTableStatistics(nullptr, table_data);
        return;
    }
}

void EloqDS::CassHandler::OnUpsertTableStatistics(CassFuture *future,
                                                  void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    if (!future || cass_future_error_code(future) == CASS_OK)
    {
        switch (table_data->op_type_)
        {
        case OperationType::CreateTable:
        case OperationType::DropTable:
        {
            const RecordSchema *rsch = table_data->schema_->RecordSchema();
            // For CREATE TABLE, will write the initial sequence record into the
            // sequence ccmap, and the record will be flush into the data store
            // during normal checkpoint, so there is no need to insert the
            // initial sequence record into data store here.
            if (rsch->AutoIncrementIndex() >= 0 &&
                table_data->op_type_ == OperationType::DropTable)
            {
#ifndef ON_KEY_OBJECT
                UpsertSequence(table_data);
#else
                assert(false);
#endif
                return;
            }
            else
            {
#ifdef RANGE_PARTITION_ENABLED
                if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
                {
                    OnPrepareTableRanges(nullptr,
                                         table_data->MarkPKTableForUpserting());
                }
                else
                {
                    PrepareTableRanges(table_data->MarkPKTableForUpserting());
                }
#endif
            }
            break;
        }
        case OperationType::AddIndex:
        case OperationType::DropIndex:
        {
#ifdef RANGE_PARTITION_ENABLED
            // Upsert index range tables
            table_data->RewindSKTableIteratorMarkFirstForUpserting();
            if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
            {
                OnPrepareTableRanges(nullptr, table_data);
            }
            else
            {
                PrepareTableRanges(table_data);
            }
#endif
            break;
        }
        default:
            LOG(ERROR)
                << "Unsupported command for CassHandler::OnUpsertCatalog";
            break;
        }

#ifndef RANGE_PARTITION_ENABLED
        table_data->hd_res_->SetFinished();
        delete table_data;
#endif
    }
    else
    {
        LOG(ERROR) << ErrorMessage(future);
        table_data->hd_res_->SetError(CcErrorCode::DATA_STORE_ERR);
        delete table_data;
    }
}

#ifndef ON_KEY_OBJECT
void EloqDS::CassHandler::UpsertSequence(UpsertTableData *table_data)
{
    CassStatement *statement = nullptr;
    CassFuture *st_future = nullptr;
    const CassCatalogInfo *cass_info = static_cast<const CassCatalogInfo *>(
        Sequences::GetTableSchema()->GetKVCatalogInfo());

    // Assign fixed partition id for sequences
    int32_t pk1 = 0;

    if (table_data->op_type_ == OperationType::DropTable)
    {
        std::string delete_str("DELETE FROM ");
        delete_str.append(table_data->cass_hd_->keyspace_name_);
        delete_str.append(".");
        delete_str.append(cass_eloq_kv_table_name);
        delete_str.append("  USING TIMESTAMP ");
        delete_str.append(std::to_string(table_data->write_time_));
        delete_str.append(" WHERE \"___mono_key___\" = 0x");
        const std::string_view table_name =
            table_data->table_name_->StringView();

        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (size_t pos = 0; pos < table_name.length(); ++pos)
        {
            ss << std::setw(2)
               << static_cast<unsigned>(
                      static_cast<uint8_t>(table_name.at(pos)));
        }

        delete_str.append(ss.str());
        // Bind table uuid
        delete_str.append(" and kvtablename = ");
        delete_str.append("'");
        delete_str.append(cass_info->kv_table_name_);
        delete_str.append("'");
        // Bind pk1
        delete_str.append(" and pk1_=");
        // delete_str.append(std::to_string(
        // Sequences::GenHashPk1(table_data->table_name_->String())));
        delete_str.append(std::to_string(pk1));
        // Bind pk2
        delete_str.append(" and pk2_=-1");

        statement = cass_statement_new(delete_str.c_str(), 0);
        st_future = cass_session_execute(table_data->session_, statement);
    }
    else
    {
        LOG(ERROR) << "Unsupported command for CassHandler::UpsertSequence.";
        return;
    }

    cass_future_set_callback(st_future, OnUpsertSequence, table_data);

    cass_future_free(st_future);
    cass_statement_free(statement);
}

void EloqDS::CassHandler::OnUpsertSequence(CassFuture *future, void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    CassError code = cass_future_error_code(future);
    if (code == CASS_OK)
    {
#ifdef RANGE_PARTITION_ENABLED
        if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
        {
            OnPrepareTableRanges(nullptr,
                                 table_data->MarkPKTableForUpserting());
        }
        else
        {
            PrepareTableRanges(table_data->MarkPKTableForUpserting());
        }
#else
        table_data->hd_res_->SetFinished();
        delete table_data;
#endif
    }
    else
    {
        LOG(ERROR) << ErrorMessage(future);
        table_data->hd_res_->SetError(CcErrorCode::DATA_STORE_ERR);
        delete table_data;
    }
}
#endif

void EloqDS::CassHandler::OnLoadRangeSlice(CassFuture *future, void *data)
{
    ScanSliceData *scan_slice_data = static_cast<ScanSliceData *>(data);

    CassError rc = cass_future_error_code(future);
    const CassResult *scan_result = nullptr;

    if (rc != CASS_OK ||
        (scan_result = cass_future_get_result(future)) == nullptr)
    {
        if (scan_slice_data->ddl_skip_kv_)
        {
            scan_slice_data->load_slice_req_->SetKvFinish(true);
        }
        else
        {
            LOG(ERROR) << "Load Range Slice failed, " << ErrorMessage(future);
            scan_slice_data->load_slice_req_->SetKvFinish(false);
        }
        delete scan_slice_data;
        return;
    }

    CassIterator *scan_it = cass_iterator_from_result(scan_result);
    FillStoreSliceCc &load_slice_req = *scan_slice_data->load_slice_req_;

    uint16_t non_key_col_cnt = 1;

    // (non_key_column ..., pk/sk, unpack_info, version, deleted)
    while (cass_iterator_next(scan_it))
    {
        const CassRow *row = cass_iterator_get_row(scan_it);

        int64_t version_ts = 0;
        cass_value_get_int64(cass_row_get_column(row, non_key_col_cnt + 2),
                             &version_ts);

        cass_bool_t is_deleted;
        cass_value_get_bool(cass_row_get_column(row, non_key_col_cnt + 3),
                            &is_deleted);

        if (is_deleted == cass_bool_t::cass_true)
        {
            assert(load_slice_req.SnapshotTs() > 0);

            if (load_slice_req.SnapshotTs() >= (uint64_t) version_ts)
            {
                continue;
            }
        }

        size_t packed_len, unpack_len;
        const cass_byte_t *packed_key, *unpack_info;

        cass_value_get_bytes(cass_row_get_column(row, non_key_col_cnt),
                             &packed_key,
                             &packed_len);

        TxKey key = TxKeyFactory::CreateTxKey(
            reinterpret_cast<const char *>(packed_key), packed_len);
        std::unique_ptr<TxRecord> record = TxRecordFactory::CreateTxRecord();
        if (!is_deleted)
        {
            const CassValue *unpack_info_value =
                cass_row_get_column(row, non_key_col_cnt + 1);
            cass_value_get_bytes(unpack_info_value, &unpack_info, &unpack_len);
            record->SetUnpackInfo(unpack_info, unpack_len);

            if (scan_slice_data->table_name_->Type() ==
                    txservice::TableType::Primary ||
                scan_slice_data->table_name_->Type() ==
                    TableType::UniqueSecondary)
            {
                const cass_byte_t *encode_blob;
                size_t encode_blob_len = 0;
                cass_value_get_bytes(cass_row_get_column(row, 0),
                                     &encode_blob,
                                     &encode_blob_len);
                record->SetEncodedBlob(encode_blob, encode_blob_len);
            }
        }
        load_slice_req.AddDataItem(
            std::move(key), std::move(record), version_ts, is_deleted);
    }

    cass_iterator_free(scan_it);

    if (cass_result_has_more_pages(scan_result))
    {
        cass_statement_set_paging_state(scan_slice_data->scan_stmt_,
                                        scan_result);
        cass_result_free(scan_result);
        CassFuture *future = cass_session_execute(scan_slice_data->session_,
                                                  scan_slice_data->scan_stmt_);
        cass_future_set_callback(future, OnLoadRangeSlice, scan_slice_data);
        cass_future_free(future);
    }
    else
    {
        cass_statement_free(scan_slice_data->scan_stmt_);
        cass_result_free(scan_result);
        load_slice_req.SetKvFinish(true);
        delete scan_slice_data;
    }
}

void EloqDS::CassHandler::UpsertInitialRangePartitionIdInternal(
    UpsertTableData *table_data,
    const txservice::TableName &table_name,
    void (*on_upsert_initial_range_partition_id_function)(CassFuture *, void *))
{
    // If ddl is skipped, skip the next_step_function, call
    // on_next_step_function directly
    if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
    {
        on_upsert_initial_range_partition_id_function(nullptr, table_data);
        return;
    }

    const txservice::TableName *tbl_name =
        table_data->GetMarkedUpsertingTableName();

    // For add index, if the cass_range_table_name meta table has initialized,
    // skip this phase.
    if (table_data->op_type_ == OperationType::CreateTable ||
        (table_data->op_type_ == OperationType::AddIndex &&
         !table_data->partition_id_initialized_.at(*tbl_name)))
    {
#ifndef ON_KEY_OBJECT
        if (tbl_name->StringView() == Sequences::mysql_seq_string)
        {
            // Assign fixed partition id for sequences range table
            table_data->initial_partition_id_.try_emplace(*tbl_name, 0);
        }
        else
#endif
        {
            table_data->initial_partition_id_.try_emplace(
                *tbl_name,
                Partition::InitialPartitionId(tbl_name->StringView()));
        }

        assert(table_name.Engine() != TableEngine::None);
        std::string table_key = table_name.Serialize();

        std::string upsert_query("INSERT INTO ");
        upsert_query.append(table_data->cass_hd_->keyspace_name_);
        upsert_query.append(".");
        upsert_query.append(cass_range_table_name);
        upsert_query.append(
            " (\"tablename\", \"___mono_key___\", \"___segment_id___\", "
            "\"___segment_cnt___\", \"___partition_id___\", "
            "\"___version___\", \"___slice_version___\") VALUES "
            "(?,?,?,?,?,?,?)");

        CassStatement *upsert_stmt =
            cass_statement_new(upsert_query.c_str(), 7);

        // Bind tablename
        cass_statement_bind_string(upsert_stmt, 0, table_key.data());
        const TxKey *packed_neg_inf_key =
            TxKeyFactory::PackedNegativeInfinity();
        // Bind mono_key
        cass_statement_bind_bytes(
            upsert_stmt,
            1,
            reinterpret_cast<const uint8_t *>(packed_neg_inf_key->Data()),
            packed_neg_inf_key->Size());  // mono_key

        // Bind segment id
        cass_statement_bind_int64(upsert_stmt, 2, 0);
        // Bind segment count
        cass_statement_bind_int64(upsert_stmt, 3, 1);
        // Bind partition_id
        cass_statement_bind_int32(
            upsert_stmt,
            4,
            table_data->initial_partition_id_.at(*tbl_name));  // partition_id
        // Bind version
        cass_statement_bind_int64(upsert_stmt,
                                  5,
                                  table_data->schema_->Version());  // version
        // Bind slice_version
        cass_statement_bind_int64(
            upsert_stmt, 6, table_data->schema_->Version());

        CassFuture *upsert_future =
            cass_session_execute(table_data->session_, upsert_stmt);
        cass_future_set_callback(upsert_future,
                                 on_upsert_initial_range_partition_id_function,
                                 table_data);
        cass_future_free(upsert_future);
        cass_statement_free(upsert_stmt);
    }
    else
    {
        on_upsert_initial_range_partition_id_function(nullptr, table_data);
    }
}

void EloqDS::CassHandler::UpsertLastRangePartitionIdInternal(
    UpsertTableData *table_data,
    const txservice::TableName &table_name,
    void (*on_upsert_last_range_partition_id_function)(CassFuture *, void *))
{
    // If ddl is skipped, skip the next_step_function, call
    // on_next_step_function directly
    if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
    {
        on_upsert_last_range_partition_id_function(nullptr, table_data);
        return;
    }

    CassStatement *stmt = nullptr;

    // tablename format: ./dbname/tablename
    if (table_data->op_type_ == OperationType::DropTable ||
        table_data->op_type_ == OperationType::DropIndex)
    {
        assert(table_name.Engine() != TableEngine::None);
        std::string table_key = table_name.Serialize();

        std::string delete_range_partition_id_str = "DELETE FROM ";
        delete_range_partition_id_str.append(
            table_data->cass_hd_->keyspace_name_);
        delete_range_partition_id_str.append(".");
        delete_range_partition_id_str.append(cass_last_range_id_name);
        delete_range_partition_id_str.append(" WHERE tablename = ?");
        stmt = cass_statement_new(delete_range_partition_id_str.c_str(), 1);
        cass_statement_bind_string(stmt, 0, table_key.data());
    }
    // For add index, if the cass_last_range_id_name meta table has initialized,
    // skip this phase.
    else if (table_data->op_type_ == OperationType::CreateTable ||
             (table_data->op_type_ == OperationType::AddIndex &&
              !table_data->partition_id_initialized_.at(table_name)))
    {
        assert(table_name.Engine() != TableEngine::None);
        std::string table_key = table_name.Serialize();

        std::string insert_range_partition_id_str = "INSERT INTO ";
        insert_range_partition_id_str.append(
            table_data->cass_hd_->keyspace_name_);
        insert_range_partition_id_str.append(".");
        insert_range_partition_id_str.append(cass_last_range_id_name);
        insert_range_partition_id_str.append(
            " (tablename, "
            "last_partition_id) VALUES (?, ?)");
        stmt = cass_statement_new(insert_range_partition_id_str.c_str(), 2);
        cass_statement_bind_string(stmt, 0, table_key.data());
        cass_statement_bind_int32(
            stmt, 1, table_data->initial_partition_id_.at(table_name));
    }
    else
    {
        on_upsert_last_range_partition_id_function(nullptr, table_data);
        return;
    }

    CassFuture *future = cass_session_execute(table_data->session_, stmt);
    cass_future_set_callback(
        future, on_upsert_last_range_partition_id_function, table_data);

    cass_future_free(future);
    cass_statement_free(stmt);
}

void EloqDS::CassHandler::OnUpsertDone(
    CassFuture *future,
    void *data,
    void (*next_step_function)(UpsertTableData *table_data),
    void (*on_next_step_function)(CassFuture *future, void *data))
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    CassError code = CASS_OK;
    if (future)
    {
        code = cass_future_error_code(future);
    }

    if (code == CASS_OK)
    {
        if (next_step_function != nullptr)
        {
            next_step_function(table_data);
        }
        else
        {
            table_data->hd_res_->SetFinished();
            delete table_data;
        }
    }
    else
    {
        LOG(ERROR) << ErrorMessage(future);
        table_data->hd_res_->SetError(CcErrorCode::DATA_STORE_ERR);
        delete table_data;
    }
}

void EloqDS::CassHandler::IterateSkIndexes(
    CassFuture *future,
    void *data,
    void (*step_function)(UpsertTableData *table_data),
    void (*on_step_function)(CassFuture *future, void *data),
    bool (*prepare_next_step_data)(UpsertTableData *table_data),
    void (*next_step_function)(UpsertTableData *table_data),
    void (*on_next_step_function)(CassFuture *future, void *data))
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    CassError code = CASS_OK;
    if (future)
    {
        code = cass_future_error_code(future);
    }

    if (code == CASS_OK)
    {
        table_data->MarkNextSKTableForUpserting();

        if (!table_data->IsSKTableIteratorEnd())
        {
            if (step_function != nullptr)
            {
                step_function(table_data);
            }
            else
            {
                table_data->hd_res_->SetFinished();
                delete table_data;
            }
        }
        else
        {
            if (prepare_next_step_data != nullptr &&
                prepare_next_step_data(table_data) &&
                next_step_function != nullptr)
            {
                next_step_function(table_data);
            }
            else
            {
                table_data->hd_res_->SetFinished();
                delete table_data;
            }
        }
    }
    else
    {
        LOG(ERROR) << ErrorMessage(future);
        table_data->hd_res_->SetError(CcErrorCode::DATA_STORE_ERR);
        delete table_data;
    }
}

void EloqDS::CassHandler::PrepareTableRanges(UpsertTableData *table_data)
{
    const txservice::TableName *table_name =
        table_data->GetMarkedUpsertingTableName();
    // If ddl is skipped, skip the next_step_function, call
    // on_next_step_function directly
    if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
    {
        OnPrepareTableRanges(nullptr, table_data);
        return;
    }

    CassStatement *create_statement = nullptr;
    if (table_data->op_type_ == OperationType::DropTable ||
        table_data->op_type_ == OperationType::DropIndex)
    {
        assert(table_name->Engine() != TableEngine::None);
        std::string table_key = table_name->Serialize();

        std::string delete_str("DELETE FROM ");
        delete_str.append(table_data->cass_hd_->keyspace_name_);
        delete_str.append(".");
        delete_str.append(cass_range_table_name);
        delete_str.append(" WHERE tablename = '");
        delete_str.append(table_key);
        delete_str.append("'");
        create_statement = cass_statement_new(delete_str.c_str(), 0);
        CassFuture *create_future =
            cass_session_execute(table_data->session_, create_statement);

        cass_future_set_callback(
            create_future, OnPrepareTableRanges, table_data);

        cass_future_free(create_future);
        cass_statement_free(create_statement);
    }
    else
    {
        OnPrepareTableRanges(nullptr, table_data);
    }
}

void EloqDS::CassHandler::OnPrepareTableRanges(CassFuture *future, void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    const txservice::TableName *upserting_table_name =
        table_data->GetMarkedUpsertingTableName();
    txservice::TableType table_type = upserting_table_name->Type();

    if (table_type == txservice::TableType::Primary)
    {
        OnUpsertDone(future,
                     data,
                     UpsertInitialRangePartitionId,
                     OnUpsertInitialRangePartitionId);
    }
    else if (table_type == txservice::TableType::Secondary ||
             table_type == txservice::TableType::UniqueSecondary)
    {
        if (table_data->op_type_ == OperationType::AddIndex)
        {
            IterateSkIndexes(future,
                             data,
                             PrepareTableRanges,
                             OnPrepareTableRanges,
                             PrepareUpsertSkTableIterator,
                             CheckTableRangesVersion,
                             OnCheckTableRangesVersion);
        }
        else
        {
            IterateSkIndexes(future,
                             data,
                             PrepareTableRanges,
                             OnPrepareTableRanges,
                             PrepareUpsertSkTableIterator,
                             UpsertInitialRangePartitionId,
                             OnUpsertInitialRangePartitionId);
        }
    }
    else
    {
        // Only primary and secondry index table have range table.
        assert(false);
    }
}

bool EloqDS::CassHandler::PrepareUpsertSkTableIterator(
    UpsertTableData *table_data)
{
    switch (table_data->op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::DropTable:
    {
        if (!table_data->HasSKTable())
        {
            return false;
        }
        break;
    }
    case OperationType::AddIndex:
    case OperationType::DropIndex:
    {
        break;
    }
    default:
        LOG(ERROR)
            << "Unsupported command for CassHandler::PrepareUpsertSkIter";
        return false;
    }

    // Upsert index range tables
    table_data->RewindSKTableIteratorMarkFirstForUpserting();
    return true;
}
void EloqDS::CassHandler::CheckTableRangesVersion(UpsertTableData *table_data)
{
    const txservice::TableName *upserting_table_name =
        table_data->GetMarkedUpsertingTableName();

    txservice::TableType table_type = upserting_table_name->Type();
    assert(table_type == txservice::TableType::Secondary ||
           table_type == txservice::TableType::UniqueSecondary);
    (void) table_type;
    // If ddl is skipped, skip the next_step_function, call
    // on_next_step_function directly
    if (table_data->ddl_skip_kv_ && !table_data->is_bootstrap_)
    {
        OnCheckTableRangesVersion(nullptr, table_data);
        return;
    }

    if (table_data->op_type_ == OperationType::AddIndex)
    {
        const txservice::TableName *table_name =
            table_data->GetMarkedUpsertingTableName();
        // For ADD INDEX, the meta table `table_ranges` may has been updated in
        // some cases, so can not re-insert the initial range partition info
        // into the table during recovery. For example: case 1) Concurrent DML
        // transaction has write new data into this table. case 2) Add index
        // transaction has write the index data which generated from the pk data
        // into this table.
        // For`table_ranges`, can not use USING TIMESTAMP to control overwrite
        // policy, because cassandra treats list type specially. So should check
        // the version manually and skip if version is the same.
        assert(table_name->Engine() != TableEngine::None);
        std::string table_key = table_name->Serialize();

        std::string select_str("SELECT \"___version___\" FROM ");
        select_str.append(table_data->cass_hd_->keyspace_name_);
        select_str.append(".");
        select_str.append(cass_range_table_name);
        select_str.append(" WHERE tablename = ? ALLOW FILTERING");
        CassStatement *select_stmt = cass_statement_new(select_str.c_str(), 1);
        cass_statement_set_is_idempotent(select_stmt, cass_true);
        cass_statement_bind_string(select_stmt, 0, table_key.data());
        CassFuture *select_future =
            cass_session_execute(table_data->session_, select_stmt);
        cass_statement_free(select_stmt);
        cass_future_set_callback(
            select_future, OnCheckTableRangesVersion, table_data);
        cass_future_free(select_future);
    }
    else
    {
        OnCheckTableRangesVersion(nullptr, table_data);
    }
}

void EloqDS::CassHandler::OnCheckTableRangesVersion(CassFuture *future,
                                                    void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    const txservice::TableName *upserting_table_name =
        table_data->GetMarkedUpsertingTableName();
    CassError rc = CASS_OK;
    if (future)
    {
        rc = cass_future_error_code(future);

        if (rc == CASS_OK)
        {
            bool has_initialized = false;
            const CassResult *result = cass_future_get_result(future);
            if (cass_result_row_count(result) > 0)
            {
                const CassRow *row = cass_result_first_row(result);
                int64_t old_version;
                cass_value_get_int64(cass_row_get_column(row, 0), &old_version);
                if (old_version >= (int64_t) table_data->schema_->Version())
                {
                    has_initialized = true;
                }
            }
            cass_result_free(result);

            table_data->partition_id_initialized_.try_emplace(
                *upserting_table_name, has_initialized);
        }
    }

    IterateSkIndexes(future,
                     data,
                     CheckTableRangesVersion,
                     OnCheckTableRangesVersion,
                     PrepareUpsertSkTableIterator,
                     UpsertInitialRangePartitionId,
                     OnUpsertInitialRangePartitionId);
}

void EloqDS::CassHandler::UpsertInitialRangePartitionId(
    UpsertTableData *table_data)
{
    const txservice::TableName &upserting_table_name =
        *table_data->GetMarkedUpsertingTableName();

    txservice::TableType table_type = upserting_table_name.Type();
    assert(table_type == txservice::TableType::Primary ||
           table_type == txservice::TableType::Secondary ||
           table_type == txservice::TableType::UniqueSecondary);
    (void) table_type;

    UpsertInitialRangePartitionIdInternal(
        table_data, upserting_table_name, OnUpsertInitialRangePartitionId);
}

void EloqDS::CassHandler::OnUpsertInitialRangePartitionId(CassFuture *future,
                                                          void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    const txservice::TableName *upserting_table_name =
        table_data->GetMarkedUpsertingTableName();
    txservice::TableType table_type = upserting_table_name->Type();

    if (table_type == txservice::TableType::Primary)
    {
        OnUpsertDone(future,
                     data,
                     UpsertLastRangePartitionId,
                     OnUpsertLastRangePartitionId);
    }
    else if (table_type == txservice::TableType::Secondary ||
             table_type == txservice::TableType::UniqueSecondary)
    {
        IterateSkIndexes(future,
                         data,
                         UpsertInitialRangePartitionId,
                         OnUpsertInitialRangePartitionId,
                         PrepareUpsertSkTableIterator,
                         UpsertLastRangePartitionId,
                         OnUpsertLastRangePartitionId);
    }
}

void EloqDS::CassHandler::UpsertLastRangePartitionId(
    UpsertTableData *table_data)
{
    const txservice::TableName &upserting_table_name =
        *table_data->GetMarkedUpsertingTableName();

    txservice::TableType table_type = upserting_table_name.Type();
    if (table_type == txservice::TableType::Primary ||
        table_type == txservice::TableType::Secondary ||
        table_type == txservice::TableType::UniqueSecondary)
    {
        UpsertLastRangePartitionIdInternal(
            table_data, upserting_table_name, OnUpsertLastRangePartitionId);
    }
    else
    {
        // Only primary and secondry index table have range table, should not
        // reach here
        assert(false);
    }
}

void EloqDS::CassHandler::OnUpsertLastRangePartitionId(CassFuture *future,
                                                       void *data)
{
    UpsertTableData *table_data = static_cast<UpsertTableData *>(data);
    const txservice::TableName *upserting_table_name =
        table_data->GetMarkedUpsertingTableName();
    txservice::TableType table_type = upserting_table_name->Type();

    if (table_type == txservice::TableType::Primary)
    {
        assert(table_data->op_type_ == OperationType::CreateTable ||
               table_data->op_type_ == OperationType::DropTable);

        OnUpsertDone(
            future,
            data,
            [](UpsertTableData *table_data)
            {
                if (table_data->HasSKTable())
                {
                    // Upsert index range tables
                    table_data->RewindSKTableIteratorMarkFirstForUpserting();
                    PrepareTableRanges(table_data);
                }
                else
                {
                    // Finish if there is not index
                    table_data->hd_res_->SetFinished();
                    delete table_data;
                }
            },
            OnPrepareTableRanges);
    }
    else if (table_type == txservice::TableType::Secondary ||
             table_type == txservice::TableType::UniqueSecondary)
    {
        IterateSkIndexes(
            future,
            data,
            UpsertLastRangePartitionId,
            OnUpsertLastRangePartitionId,
            [](UpsertTableData *table_data) { return false; },
            nullptr,
            nullptr);
    }
    else
    {
        // Only primary and secondry index table have range table.
        assert(false);
    }
}

/**
 * @brief Fetch table catalog from table_catalogs.
 * Note that ccm_table_name should be base table name with format
 * ./dbname/tablename.
 *
 */
void EloqDS::CassHandler::FetchTableCatalog(
    const txservice::TableName &ccm_table_name,
    txservice::FetchCatalogCc *fetch_cc)
{
    std::string table_key = ccm_table_name.Serialize();
    std::string query(
        "SELECT content, version, kvtablename, "
        "kvindexname, keyschemasts FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_table_catalog_name);
    query.append(" WHERE tablename='");
    query.append(table_key);
    query.append("'");

    CassStatement *fetch_stmt = cass_statement_new(query.c_str(), 0);
    CassFuture *fetch_future = cass_session_execute(session_, fetch_stmt);

    cass_future_set_callback(fetch_future, OnFetchCatalog, fetch_cc);

    cass_future_free(fetch_future);
    cass_statement_free(fetch_stmt);
}

void EloqDS::CassHandler::OnFetchCatalog(CassFuture *future, void *fetch_req)
{
    txservice::FetchCatalogCc *fetch_cc =
        static_cast<txservice::FetchCatalogCc *>(fetch_req);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
    {
        LOG(ERROR) << ErrorMessage(future);
        fetch_cc->SetFinish(txservice::RecordStatus::Unknown,
                            static_cast<int>(CcErrorCode::DATA_STORE_ERR));
        return;
    }

    const CassResult *result = cass_future_get_result(future);
    // The error code would be non-zero, if the result is nullptr.
    assert(result != nullptr);

    const CassRow *row = cass_result_first_row(result);
    if (row != nullptr)
    {
        std::string &catalog_image = fetch_cc->CatalogImage();
        uint64_t &commit_ts = fetch_cc->CommitTs();
        const char *item;
        size_t item_length;
        cass_value_get_string(cass_row_get_column(row, 0), &item, &item_length);
        std::string frm(item, item_length);
        cass_value_get_int64(cass_row_get_column(row, 1),
                             (int64_t *) &commit_ts);
        cass_value_get_string(cass_row_get_column(row, 2), &item, &item_length);
        std::string kv_table_name(item, item_length);
        cass_value_get_string(cass_row_get_column(row, 3), &item, &item_length);
        std::string kv_index_names(item, item_length);
        cass_value_get_string(cass_row_get_column(row, 4), &item, &item_length);
        std::string key_schemas_ts(item, item_length);

        catalog_image.append(SerializeSchemaImage(
            frm,
            CassCatalogInfo(kv_table_name, kv_index_names).Serialize(),
            TableKeySchemaTs(key_schemas_ts, fetch_cc->CatalogName().Engine())
                .Serialize()));

        fetch_cc->SetFinish(txservice::RecordStatus::Normal, 0);
    }
    else
    {
        // CommitTs= 1 indicate non-existence
        fetch_cc->CatalogImage().clear();
        fetch_cc->CommitTs() = 1;
        fetch_cc->SetFinish(txservice::RecordStatus::Deleted, 0);
    }

    cass_result_free(result);
}

void EloqDS::CassHandler::FetchCurrentTableStatistics(
    const txservice::TableName &ccm_table_name,
    FetchTableStatisticsCc *fetch_cc)
{
    fetch_cc->SetStoreHandler(this);

    assert(ccm_table_name.Engine() != TableEngine::None);
    std::string table_key = ccm_table_name.Serialize();

    std::string query_str("SELECT version FROM ");
    query_str.append(keyspace_name_);
    query_str.append(".");
    query_str.append(cass_table_statistics_version_name);
    query_str.append(" WHERE tablename=?");
    CassStatement *query_stmt = cass_statement_new(query_str.c_str(), 1);
    cass_statement_bind_string_n(
        query_stmt, 0, table_key.data(), table_key.size());
    CassFuture *cass_future = cass_session_execute(session_, query_stmt);
    cass_future_set_callback(
        cass_future, OnFetchCurrentTableStatistics, fetch_cc);

    cass_future_free(cass_future);
    cass_statement_free(query_stmt);
}

void EloqDS::CassHandler::OnFetchCurrentTableStatistics(CassFuture *future,
                                                        void *fetch_req)
{
    txservice::FetchTableStatisticsCc *fetch_cc =
        static_cast<txservice::FetchTableStatisticsCc *>(fetch_req);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
    {
        LOG(ERROR) << ErrorMessage(future);
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        return;
    }

    const CassResult *result = cass_future_get_result(future);
    const CassRow *row = cass_result_first_row(result);
    if (row)
    {
        const CassValue *version_val =
            cass_row_get_column_by_name(row, "version");
        cass_int64_t version_i64 = 0;
        cass_value_get_int64(version_val, &version_i64);
        fetch_cc->SetCurrentVersion(static_cast<uint64_t>(version_i64));
        fetch_cc->StoreHandler()->FetchTableStatistics(fetch_cc->CatalogName(),
                                                       fetch_cc);
    }
    else
    {
        // empty statistics
        fetch_cc->SetFinish(0);
    }
    cass_result_free(result);
}

void EloqDS::CassHandler::FetchTableStatistics(
    const txservice::TableName &ccm_table_name,
    FetchTableStatisticsCc *fetch_cc)
{
    assert(ccm_table_name.Engine() != TableEngine::None);
    std::string table_key = ccm_table_name.Serialize();

    std::string query_str(
        "SELECT indextype, indexname, records, samplekeys FROM ");
    query_str.append(keyspace_name_);
    query_str.append(".");
    query_str.append(cass_table_statistics_name);
    query_str.append(" WHERE tablename=? AND version=?");
    CassStatement *query_stmt = cass_statement_new(query_str.c_str(), 2);

    cass_statement_bind_string_n(
        query_stmt, 0, table_key.data(), table_key.size());
    cass_statement_bind_int64(query_stmt, 1, fetch_cc->CurrentVersion());

    CassFuture *cass_future = cass_session_execute(session_, query_stmt);

    cass_future_set_callback(cass_future, OnFetchTableStatistics, fetch_cc);

    cass_future_free(cass_future);
    cass_statement_free(query_stmt);
}

void EloqDS::CassHandler::OnFetchTableStatistics(CassFuture *future,
                                                 void *fetch_req)
{
    txservice::FetchTableStatisticsCc *fetch_cc =
        static_cast<txservice::FetchTableStatisticsCc *>(fetch_req);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
    {
        LOG(ERROR) << ErrorMessage(future);
        fetch_cc->SetFinish(
            static_cast<int>(txservice::CcErrorCode::DATA_STORE_ERR));
        return;
    }

    const CassResult *result = cass_future_get_result(future);
    CassIterator *iter = cass_iterator_from_result(result);
    while (cass_iterator_next(iter) == cass_bool_t::cass_true)
    {
        const CassRow *row = cass_iterator_get_row(iter);

        const CassValue *indextype_val =
            cass_row_get_column_by_name(row, "indextype");
        cass_int8_t indextype_i8 = 0;
        cass_value_get_int8(indextype_val, &indextype_i8);
        TableType indextype = static_cast<TableType>(indextype_i8);

        const CassValue *indexname_val =
            cass_row_get_column_by_name(row, "indexname");
        const char *indexname_ptr = nullptr;
        size_t indexname_len = 0;
        cass_value_get_string(indexname_val, &indexname_ptr, &indexname_len);
        std::string indexname_str(indexname_ptr, indexname_len);

        TableName indexname(std::move(indexname_str),
                            indextype,
                            fetch_cc->CatalogName().Engine());

        cass_int64_t records_i64 = 0;
        const CassValue *records_val =
            cass_row_get_column_by_name(row, "records");
        cass_value_get_int64(records_val, &records_i64);
        if (records_i64 >= 0)
        {
            uint64_t records = static_cast<uint64_t>(records_i64);
            fetch_cc->SetRecords(indexname, records);
        }

        std::vector<TxKey> samplekeys;
        const CassValue *samplekeys_val =
            cass_row_get_column_by_name(row, "samplekeys");
        CassIterator *it = cass_iterator_from_collection(samplekeys_val);
        while (cass_iterator_next(it) == cass_bool_t::cass_true)
        {
            const CassValue *samplekey_val = cass_iterator_get_value(it);
            const cass_byte_t *samplekey_ptr = nullptr;
            size_t samplekey_len = 0;
            cass_value_get_bytes(samplekey_val, &samplekey_ptr, &samplekey_len);
            TxKey samplekey = TxKeyFactory::CreateTxKey(
                reinterpret_cast<const char *>(samplekey_ptr), samplekey_len);
            samplekeys.emplace_back(std::move(samplekey));
        }
        cass_iterator_free(it);

        fetch_cc->SamplePoolMergeFrom(indexname, std::move(samplekeys));
    }
    cass_iterator_free(iter);

    fetch_cc->SetFinish(0);

    cass_result_free(result);
}

// Both cassandra and dynamodb have some limitations on collection size/row
// bytes. Each node group contains a sample pool, when write them to storage,
// we merge them together. The merged sample pool may be too large to store in
// one row. Therefore, we have to store table statistics segmentally.
//
// A example for table t1(i int primary key, j int, key(j)) stores as follows:
//
// head: |tablename|version|indextype|indexname|segment_id|records|sample_keys|
// row0: |t1       |ckpt_ts|        0|       t1|         0|     -1|        xxx|
// row1: |t1       |ckpt_ts|        0|       t1|         1|     -1|        xxx|
// row2: |t1       |ckpt_ts|        0|       t1|         2|  10000|        xxx|
// row3: |t1       |ckpt_ts|        1|     t1$j|         0|     -1|        xxx|
// row4: |t1       |ckpt_ts|        1|     t1$j|         1|     -1|        xxx|
// row5: |t1       |ckpt_ts|        1|     t1$j|         2|  10000|        xxx|
//
// records column with value great or equal to zero indicates end a
// table/index.
//
bool EloqDS::CassHandler::UpsertTableStatistics(
    const txservice::TableName &ccm_table_name,
    const std::unordered_map<TableName,
                             std::pair<uint64_t, std::vector<txservice::TxKey>>>
        &sample_pool_map,
    uint64_t version)
{
    assert(ccm_table_name.Engine() != TableEngine::None);
    std::string table_key = ccm_table_name.Serialize();

    {
        std::string insert_str = "INSERT INTO ";
        insert_str.append(keyspace_name_);
        insert_str.append(".");
        insert_str.append(cass_table_statistics_name);
        insert_str.append(
            " (tablename, version, indextype, "
            "indexname, segment_id, records, samplekeys) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)");
        CassStatement *insert_stmt = cass_statement_new(insert_str.c_str(), 7);
        cass_statement_bind_string_n(
            insert_stmt, 0, table_key.data(), table_key.size());
        cass_statement_bind_int64(
            insert_stmt, 1, static_cast<int64_t>(version));

        for (const auto &[indexname, sample_pool] : sample_pool_map)
        {
            cass_statement_bind_int8(
                insert_stmt, 2, static_cast<cass_int8_t>(indexname.Type()));
            cass_statement_bind_string_n(insert_stmt,
                                         3,
                                         indexname.StringView().data(),
                                         indexname.StringView().size());
            CassCollection *collection = cass_collection_new(
                CassCollectionType::CASS_COLLECTION_TYPE_SET, 1024);

            uint32_t segment_id = 0;
            uint32_t segment_size = 0;
            size_t sz = sample_pool.second.size();
            for (size_t i = 0; i < sz; ++i)
            {
                const txservice::TxKey &samplekey = sample_pool.second[i];

                if (segment_size + samplekey.Size() >= collection_max_size_)
                {
                    cass_statement_bind_int32(
                        insert_stmt, 4, static_cast<cass_int32_t>(segment_id));
                    // set records to -1 means uncomplete write.
                    cass_statement_bind_int64(insert_stmt, 5, -1);
                    cass_statement_bind_collection(insert_stmt, 6, collection);

                    CassFuture *insert_future =
                        cass_session_execute(session_, insert_stmt);
                    if (!cass_future_wait_timed(insert_future,
                                                future_wait_timeout))
                    {
                        LOG(ERROR)
                            << "Insert table_statistics timed out, "
                            << ", tablename: " << ccm_table_name.StringView();
                        cass_future_free(insert_future);
                        cass_collection_free(collection), collection = nullptr;
                        cass_statement_free(insert_stmt);
                        return false;
                    }
                    CassError rc = cass_future_error_code(insert_future);
                    if (rc != CassError::CASS_OK)
                    {
                        LOG(ERROR)
                            << "Insert table_statistics failed, "
                            << "error code: " << rc << ", "
                            << "error message: " << ErrorMessage(insert_future)
                            << ", tablename: " << ccm_table_name.StringView();
                        cass_future_free(insert_future);
                        cass_collection_free(collection), collection = nullptr;
                        cass_statement_free(insert_stmt);
                        return false;
                    }

                    cass_future_free(insert_future);
                    cass_collection_free(collection), collection = nullptr;

                    collection = cass_collection_new(
                        CassCollectionType::CASS_COLLECTION_TYPE_SET, 1024);
                    segment_id += 1;
                    segment_size = 0;
                }

                segment_size += samplekey.Size();
                cass_collection_append_bytes(
                    collection,
                    reinterpret_cast<const cass_byte_t *>(samplekey.Data()),
                    samplekey.Size());
                if (i == sz - 1)
                {
                    cass_statement_bind_int32(
                        insert_stmt, 4, static_cast<cass_int32_t>(segment_id));
                    cass_statement_bind_int64(
                        insert_stmt, 5, sample_pool.first);
                    cass_statement_bind_collection(insert_stmt, 6, collection);

                    CassFuture *insert_future =
                        cass_session_execute(session_, insert_stmt);
                    if (!cass_future_wait_timed(insert_future,
                                                future_wait_timeout))
                    {
                        LOG(ERROR)
                            << "Insert table_statistics timed out, "
                            << ", tablename: " << ccm_table_name.StringView();
                        cass_future_free(insert_future);
                        cass_collection_free(collection), collection = nullptr;
                        cass_statement_free(insert_stmt);
                        return false;
                    }
                    CassError rc = cass_future_error_code(insert_future);
                    if (rc != CassError::CASS_OK)
                    {
                        LOG(ERROR)
                            << "Insert table_statistics failed, "
                            << "error code: " << rc << ", "
                            << "error message: " << ErrorMessage(insert_future)
                            << ", tablename: " << ccm_table_name.StringView();
                        cass_future_free(insert_future);
                        cass_collection_free(collection), collection = nullptr;
                        cass_statement_free(insert_stmt);
                        return false;
                    }

                    cass_future_free(insert_future);
                    cass_collection_free(collection), collection = nullptr;
                }
            }

            if (collection)
            {
                cass_collection_free(collection), collection = nullptr;
            }
        }
        cass_statement_free(insert_stmt);
    }

    {
        std::string upsert_str = "Insert into ";
        upsert_str.append(keyspace_name_);
        upsert_str.append(".");
        upsert_str.append(cass_table_statistics_version_name);
        upsert_str.append(" (tablename, version) VALUES (?, ?)");
        CassStatement *upsert_stmt = cass_statement_new(upsert_str.c_str(), 2);
        cass_statement_bind_string_n(
            upsert_stmt, 0, table_key.data(), table_key.size());
        cass_statement_bind_int64(
            upsert_stmt, 1, static_cast<int64_t>(version));
        CassFuture *upsert_future = cass_session_execute(session_, upsert_stmt);
        if (!cass_future_wait_timed(upsert_future, future_wait_timeout))
        {
            LOG(ERROR) << "Delete expired table_statistics timed out "
                       << "tablename: " << ccm_table_name.StringView();
            cass_future_free(upsert_future);
            cass_statement_free(upsert_stmt);
            return false;
        }
        CassError rc = cass_future_error_code(upsert_future);
        if (rc != CassError::CASS_OK)
        {
            LOG(ERROR) << "Upsert table_statistics_version failed, "
                       << "error code: " << rc << ", "
                       << "error message: " << ErrorMessage(upsert_future)
                       << ", "
                       << "tablename: " << ccm_table_name.StringView();
            cass_future_free(upsert_future);
            cass_statement_free(upsert_stmt);
            return false;
        }

        cass_future_free(upsert_future);
        cass_statement_free(upsert_stmt);
    }

    {
        std::string delete_str = "DELETE FROM ";
        delete_str.append(keyspace_name_);
        delete_str.append(".");
        delete_str.append(cass_table_statistics_name);
        delete_str.append(" WHERE tablename = ? AND version < ?");
        CassStatement *delete_stmt = cass_statement_new(delete_str.c_str(), 2);
        cass_statement_bind_string_n(
            delete_stmt, 0, table_key.data(), table_key.size());
        cass_statement_bind_int64(
            delete_stmt, 1, static_cast<int64_t>(version));
        CassFuture *delete_future = cass_session_execute(session_, delete_stmt);
        if (!cass_future_wait_timed(delete_future, future_wait_timeout))
        {
            LOG(ERROR) << "Delete expired table_statistics timed out "
                       << "tablename: " << ccm_table_name.StringView();
            cass_future_free(delete_future);
            cass_statement_free(delete_stmt);
            return false;
        }
        CassError rc = cass_future_error_code(delete_future);
        if (rc != CassError::CASS_OK)
        {
            LOG(ERROR) << "Delete expired table_statistics failed, "
                       << "error code: " << rc << ", "
                       << "error message: " << ErrorMessage(delete_future)
                       << ", "
                       << "tablename: " << ccm_table_name.StringView();
            cass_future_free(delete_future);
            cass_statement_free(delete_stmt);
            return false;
        }

        cass_future_free(delete_future);
        cass_statement_free(delete_stmt);
    }

    return true;
}

void EloqDS::CassHandler::FetchTableRanges(
    txservice::FetchTableRangesCc *fetch_cc)
{
    assert(fetch_cc->table_name_.Engine() != TableEngine::None);
    std::string table_key = fetch_cc->table_name_.Serialize();

    std::string query(
        "SELECT \"___mono_key___\", \"___partition_id___\", "
        "\"___segment_id___\", \"___version___\" FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_range_table_name);
    query.append(
        " WHERE tablename=? AND \"___segment_id___\" = 0 ALLOW FILTERING");
    CassStatement *fetch_stmt = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(fetch_stmt, 0, table_key.data());
    cass_statement_set_paging_size(fetch_stmt, 50);
    cass_statement_set_is_idempotent(fetch_stmt, cass_true);
    FetchRangeSpecData *fetch_data =
        new FetchRangeSpecData(fetch_cc, session_, fetch_stmt);
    CassFuture *fetch_future = cass_session_execute(session_, fetch_stmt);
    cass_future_set_callback(fetch_future, OnFetchTableRanges, fetch_data);

    cass_future_free(fetch_future);
}

void EloqDS::CassHandler::OnFetchTableRanges(CassFuture *future,
                                             void *fetch_req)
{
    FetchRangeSpecData *fetch_data =
        static_cast<FetchRangeSpecData *>(fetch_req);
    txservice::FetchTableRangesCc *fetch_range_cc = fetch_data->fetch_cc_;

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
    {
        LOG(ERROR) << "Fetch table range failed: " << ErrorMessage(future);
        cass_statement_free(fetch_data->stmt_);
        fetch_range_cc->SetFinish(rc);
        delete fetch_data;
        return;
    }

    std::vector<txservice::InitRangeEntry> range_vec;

    const CassResult *result = cass_future_get_result(future);
    CassIterator *iter = cass_iterator_from_result(result);

    LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
    std::unique_lock<std::mutex> heap_lk(shards->table_ranges_heap_mux_);
    bool is_override_thd = mi_is_override_thread();
    mi_threadid_t prev_thd =
        mi_override_thread(shards->GetTableRangesHeapThreadId());
    mi_heap_t *prev_heap = mi_heap_set_default(shards->GetTableRangesHeap());

    while (cass_iterator_next(iter) == cass_bool_t::cass_true)
    {
        const CassRow *row = cass_iterator_get_row(iter);
        const CassValue *mono_key_val =
            cass_row_get_column_by_name(row, "\"___mono_key___\"");
        const cass_byte_t *mono_key_ptr = nullptr;
        size_t mono_key_len;
        cass_value_get_bytes(mono_key_val, &mono_key_ptr, &mono_key_len);

        const CassValue *pt_cass_val =
            cass_row_get_column_by_name(row, "___partition_id___");
        cass_int32_t partition_id = 0;
        if (pt_cass_val != nullptr && !cass_value_is_null(pt_cass_val))
        {
            cass_value_get_int32(pt_cass_val, &partition_id);
        }

        const CassValue *vt_cass_val =
            cass_row_get_column_by_name(row, "___version___");
        cass_int64_t val;
        cass_value_get_int64(vt_cass_val, &val);
        uint64_t version_ts = val;

        // Range infos are stored in order of range start keys so we don't need
        // to worry about sorting them.
        // The first range always starts with negative infinity.
        if (mono_key_len > 1 || *mono_key_ptr != 0x00)
        {
            TxKey start_key = TxKeyFactory::CreateTxKey(
                reinterpret_cast<const char *>(mono_key_ptr), mono_key_len);
            range_vec.emplace_back(
                std::move(start_key), partition_id, version_ts);
        }
        else
        {
            range_vec.emplace_back(
                TxKeyFactory::NegInfTxKey()->GetShallowCopy(),
                partition_id,
                version_ts);
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

    if (cass_result_has_more_pages(result))
    {
        fetch_range_cc->AppendTableRanges(std::move(range_vec));
        cass_statement_set_paging_state(fetch_data->stmt_, result);
        CassFuture *future =
            cass_session_execute(fetch_data->session_, fetch_data->stmt_);
        cass_future_set_callback(future, OnFetchTableRanges, fetch_data);
        cass_future_free(future);
    }
    else
    {
        cass_statement_free(fetch_data->stmt_);
        // When ddl_skip_kv_ is enabled and the range entry is not physically
        // ready, initializes the original range from negative infinity to
        // positive infinity.
        if (range_vec.empty() && fetch_range_cc->EmptyRanges())
        {
            range_vec.emplace_back(
                TxKeyFactory::NegInfTxKey()->GetShallowCopy(),
                Partition::InitialPartitionId(
                    fetch_range_cc->table_name_.StringView()),
                1);
        }
        fetch_range_cc->AppendTableRanges(std::move(range_vec));
        fetch_range_cc->SetFinish(0);
        delete fetch_data;
    }

    cass_iterator_free(iter);
    cass_result_free(result);
}

void EloqDS::CassHandler::FetchRangeSlices(
    txservice::FetchRangeSlicesReq *fetch_cc)
{
    if (Sharder::Instance().TryPinNodeGroupData(fetch_cc->cc_ng_id_) !=
        fetch_cc->cc_ng_term_)
    {
        fetch_cc->SetFinish(CcErrorCode::NG_TERM_CHANGED);
        return;
    }

    assert(fetch_cc->table_name_.Engine() != TableEngine::None);
    std::string table_key = fetch_cc->table_name_.Serialize();

    std::string query(
        "SELECT \"___slice_keys___\", \"___slice_sizes___\", "
        "\"___slice_version___\", \"___segment_cnt___\" FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_range_table_name);
    query.append(" WHERE tablename=? AND \"___mono_key___\"=? ALLOW FILTERING");
    CassStatement *fetch_stmt = cass_statement_new(query.c_str(), 2);
    cass_statement_bind_string(fetch_stmt, 0, table_key.data());

    TxKey start_key = fetch_cc->range_entry_->GetRangeInfo()->StartTxKey();
    if (start_key.Type() == txservice::KeyType::Normal)
    {
        // bind mono_key
        cass_statement_bind_bytes(
            fetch_stmt,
            1,
            reinterpret_cast<const uint8_t *>(start_key.Data()),
            start_key.Size());
    }
    else
    {
        const TxKey *packed_neg_inf_key = TxKeyFactory::NegInfTxKey();
        // bind mono_key
        cass_statement_bind_bytes(
            fetch_stmt,
            1,
            reinterpret_cast<const uint8_t *>(packed_neg_inf_key->Data()),
            packed_neg_inf_key->Size());  // mono_key
    }

    cass_statement_set_paging_size(fetch_stmt, 1);
    CassFuture *fetch_future = cass_session_execute(session_, fetch_stmt);
    FetchSlicesSpecData *fetch_data =
        new FetchSlicesSpecData(fetch_cc, session_, fetch_stmt);
    cass_future_set_callback(fetch_future, OnFetchRangeSlices, fetch_data);

    cass_future_free(fetch_future);
    cass_statement_free(fetch_stmt);
}

void EloqDS::CassHandler::OnFetchRangeSlices(CassFuture *future,
                                             void *fetch_req)
{
    FetchSlicesSpecData *fetch_data =
        static_cast<FetchSlicesSpecData *>(fetch_req);
    std::unique_ptr<FetchSlicesSpecData> fetch_data_guard(fetch_data);

    FetchRangeSlicesReq *fetch_cc = fetch_data->fetch_cc_;
    NodeGroupId ng_id = fetch_cc->cc_ng_id_;
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
    {
        LOG(ERROR) << "Fetch table range failed: " << ErrorMessage(future);
        fetch_cc->SetFinish(CcErrorCode::DATA_STORE_ERR);
        Sharder::Instance().UnpinNodeGroupData(ng_id);
        return;
    }

    const CassResult *result = cass_future_get_result(future);
    size_t row_count = cass_result_row_count(result);
    if (row_count > 0)
    {
        uint64_t slice_version = 0;
        uint64_t current_segment_id = fetch_cc->CurrentSegmentId();
        uint64_t segment_cnt = 0;
        const CassRow *first_row = cass_result_first_row(result);

        // first segment
        if (current_segment_id == 0)
        {
            const CassValue *slice_version_val =
                cass_row_get_column_by_name(first_row, "___slice_version___");
            const CassValue *segment_cnt_val =
                cass_row_get_column_by_name(first_row, "___segment_cnt___");
            cass_int64_t slice_version_i64 = 0;
            cass_int64_t segment_cnt_i64 = 0;
            cass_value_get_int64(slice_version_val, &slice_version_i64);
            cass_value_get_int64(segment_cnt_val, &segment_cnt_i64);
            slice_version = static_cast<uint64_t>(slice_version_i64);
            segment_cnt = static_cast<uint64_t>(segment_cnt_i64);
            // Store slice_version and segment_cnt. We check these variables to
            // avoid reading incomplete results
            fetch_cc->SetSegmentCnt(segment_cnt);
            fetch_cc->SetSliceVersion(slice_version);
        }
        else
        {
            segment_cnt = fetch_cc->SegmentCnt();
            slice_version = fetch_cc->SliceVersion();
        }

        if (current_segment_id == 0)
        {
            const CassValue *slice_sizes_val =
                cass_row_get_column_by_name(first_row, "___slice_sizes___");

            // New Table
            if (slice_sizes_val == nullptr ||
                cass_value_is_null(slice_sizes_val))
            {
                // Free CassResult
                cass_result_free(result);

                assert(segment_cnt == 1);
                fetch_cc->slice_info_.emplace_back(
                    TxKey(), 0, SliceStatus::PartiallyCached);
                fetch_cc->SetFinish(CcErrorCode::NO_ERROR);
                Sharder::Instance().UnpinNodeGroupData(ng_id);
                return;
            }
        }

        // Iterator each row
        CassIterator *iter = cass_iterator_from_result(result);
        while (cass_iterator_next(iter) == cass_bool_t::cass_true)
        {
            const CassRow *row = cass_iterator_get_row(iter);
            const CassValue *row_slice_version_val =
                cass_row_get_column_by_name(row, "___slice_version___");
            cass_int64_t row_slice_version_i64 = 0;
            cass_value_get_int64(row_slice_version_val, &row_slice_version_i64);
            if (static_cast<uint64_t>(row_slice_version_i64) != slice_version)
            {
                LOG(ERROR) << "Fetch range slices failed: mismatch "
                           << ", item_slice_version: "
                           << static_cast<uint64_t>(row_slice_version_i64)
                           << ", slice_verison: " << slice_version;
                // Free CassIterator CassResult
                cass_iterator_free(iter);
                cass_result_free(result);
                fetch_cc->SetFinish(CcErrorCode::DATA_STORE_ERR);
                Sharder::Instance().UnpinNodeGroupData(ng_id);
                return;
            }

            size_t first_index = fetch_cc->slice_info_.size();

            const CassValue *slice_sizes_val =
                cass_row_get_column_by_name(row, "___slice_sizes___");
            const CassValue *slice_keys_val =
                cass_row_get_column_by_name(row, "___slice_keys___");

            const cass_byte_t *slice_size_val_raw_ptr = nullptr;
            size_t slice_size_val_raw_size = 0;
            cass_value_get_bytes(slice_sizes_val,
                                 &slice_size_val_raw_ptr,
                                 &slice_size_val_raw_size);

            size_t offset = 0;
            while (offset < slice_size_val_raw_size)
            {
                uint32_t slice_size = *reinterpret_cast<const uint32_t *>(
                    slice_size_val_raw_ptr + offset);
                fetch_cc->slice_info_.emplace_back(
                    TxKey(), slice_size, SliceStatus::PartiallyCached);
                offset += sizeof(uint32_t);
            }

            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> heap_lk(
                shards->table_ranges_heap_mux_);
            bool is_override_thd = mi_is_override_thread();
            mi_threadid_t prev_thd =
                mi_override_thread(shards->GetTableRangesHeapThreadId());
            mi_heap_t *prev_heap =
                mi_heap_set_default(shards->GetTableRangesHeap());

            size_t vec_index = first_index;
            if (current_segment_id == 0)
            {
                assert(vec_index == 0);
                vec_index = 1;
            }

            const cass_byte_t *slice_key_val_raw_ptr = nullptr;
            size_t slice_key_val_raw_size = 0;
            cass_value_get_bytes(slice_keys_val,
                                 &slice_key_val_raw_ptr,
                                 &slice_key_val_raw_size);

            offset = 0;
            while (offset < slice_key_val_raw_size)
            {
                uint32_t key_len =
                    *reinterpret_cast<const uint32_t *>(slice_key_val_raw_ptr);
                slice_key_val_raw_ptr += sizeof(uint32_t);

                fetch_cc->slice_info_[vec_index].key_ =
                    TxKeyFactory::CreateTxKey(
                        reinterpret_cast<const char *>(slice_key_val_raw_ptr),
                        key_len);
                slice_key_val_raw_ptr += key_len;

                offset += sizeof(uint32_t) + key_len;
                vec_index++;
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

            current_segment_id++;
            if (current_segment_id == segment_cnt)
            {
                cass_iterator_free(iter);
                cass_result_free(result);
                fetch_cc->SetFinish(CcErrorCode::NO_ERROR);
                Sharder::Instance().UnpinNodeGroupData(ng_id);
                return;
            }
        }

        cass_iterator_free(iter);

        if (current_segment_id == segment_cnt)
        {
            cass_result_free(result);
            fetch_cc->SetFinish(CcErrorCode::NO_ERROR);
            Sharder::Instance().UnpinNodeGroupData(ng_id);
            return;
        }

        if (cass_result_has_more_pages(result) == false)
        {
            // Partial result.
            LOG(ERROR)
                << "Fetch range slices failed: Partial result, keey retring";
            cass_result_free(result);
            fetch_cc->SetFinish(CcErrorCode::DATA_STORE_ERR);
            Sharder::Instance().UnpinNodeGroupData(ng_id);
            return;
        }

        // Release ownership
        fetch_data = fetch_data_guard.release();

        fetch_cc->SetCurrentSegmentId(current_segment_id);
        cass_statement_set_paging_state(fetch_data->stmt_, result);
        CassFuture *new_future =
            cass_session_execute(fetch_data->session_, fetch_data->stmt_);
        cass_future_set_callback(new_future, OnFetchRangeSlices, fetch_data);
        // Free new future
        cass_future_free(new_future);
    }
    else
    {
        if (fetch_cc->slice_info_.empty())
        {
            // This should only happen if ddl_skip_kv_ is true.
            fetch_cc->slice_info_.emplace_back(
                TxKey(), 0, SliceStatus::PartiallyCached);
            fetch_cc->SetFinish(CcErrorCode::NO_ERROR);
        }
        else
        {
            // Partial result.
            LOG(ERROR)
                << "Fetch range slices failed: Partial result, keey retring";
            fetch_cc->SetFinish(CcErrorCode::DATA_STORE_ERR);
        }
    }

    cass_result_free(result);
    Sharder::Instance().UnpinNodeGroupData(ng_id);
}

txservice::store::DataStoreHandler::DataStoreOpStatus
EloqDS::CassHandler::FetchRecord(txservice::FetchRecordCc *fetch_cc)
{
    const CassPrepared *read_prepared = GetCachedPreparedStmt(
        cass_eloq_kv_table_name, 0, CassPreparedType::Read);

    if (read_prepared == nullptr)
    {
        if (!CreateCachedPrepareStmt(
                cass_eloq_kv_table_name, 0, CassPreparedType::Read))
        {
            // prepared stmt for this table is being created, wait and retry
            // later.
            return store::DataStoreHandler::DataStoreOpStatus::Retry;
        }

        std::string read_str("SELECT ");
        read_str.append(" \"___encoded_blob___\", ");
        read_str.append(
            " \"___mono_key___\", \"___unpack_info___\", \"___version___\", "
            "\"___deleted___\" FROM ");
        read_str.append(keyspace_name_);
        read_str.append(".");
        read_str.append(cass_eloq_kv_table_name);
        read_str.append(
            " WHERE kvtablename =? AND pk1_=? AND pk2_=? AND "
            "\"___mono_key___\"=?");

        CassFuture *prepare_future =
            cass_session_prepare(session_, read_str.c_str());
        // Make fetch cc the owner of tx key so that the key won't be invalid on
        // callback.
        if (!fetch_cc->tx_key_.IsOwner())
        {
            fetch_cc->tx_key_ = fetch_cc->tx_key_.Clone();
        }
        FetchRecordData *fetch_data = new FetchRecordData(fetch_cc, this);
        cass_future_set_callback(
            prepare_future,
            [](CassFuture *future, void *data)
            {
                FetchRecordData *fetch_data =
                    static_cast<FetchRecordData *>(data);
                FetchRecordCc *fetch_cc = fetch_data->fetch_cc_;
                CassError rc = cass_future_error_code(future);
                if (rc != CASS_OK)
                {
                    if (fetch_data->handler_->ddl_skip_kv_)
                    {
                        fetch_cc->SetFinish(0);
                    }
                    else
                    {
                        LOG(ERROR) << "Failed to create prepare statement for "
                                      "fetch record , "
                                   << ErrorMessage(future);
                        fetch_cc->SetFinish(
                            static_cast<int>(CcErrorCode::DATA_STORE_ERR));
                    }
                    delete fetch_data;
                    return;
                }

                auto prepared = cass_future_get_prepared(future);

                fetch_data->handler_->CachePreparedStmt(cass_eloq_kv_table_name,
                                                        0,
                                                        prepared,
                                                        CassPreparedType::Read);

                fetch_data->handler_->FetchRecord(fetch_data->fetch_cc_);

                delete fetch_data;
            },
            fetch_data);
        cass_future_free(prepare_future);
        return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
    }
    // set cass read start time
    if (metrics::enable_kv_metrics)
    {
        fetch_cc->start_ = metrics::Clock::now();
    }

    CassStatement *statement = cass_prepared_bind(read_prepared);
    cass_statement_set_is_idempotent(statement, cass_true);
    // Bind kv table name
    cass_statement_bind_string(statement,
                               0,
                               fetch_cc->table_schema_->GetKVCatalogInfo()
                                   ->GetKvTableName(*fetch_cc->table_name_)
                                   .data());

    int32_t pk1 = fetch_cc->range_id_;
    int16_t pk2 = -1;

    // Bind pk1_
    cass_statement_bind_int32(statement, 1, pk1);
    // Bind pk2_
    cass_statement_bind_int16(statement, 2, pk2);

    // Binds the key field
    cass_statement_bind_bytes(
        statement,
        3,
        reinterpret_cast<const uint8_t *>(fetch_cc->tx_key_.Data()),
        fetch_cc->tx_key_.Size());

    CassFuture *future = cass_session_execute(session_, statement);
    cass_future_set_callback(
        future, OnFetchRecord, new FetchRecordData(fetch_cc, this));
    cass_future_free(future);
    cass_statement_free(statement);

    return store::DataStoreHandler::DataStoreOpStatus::Success;
}

void EloqDS::CassHandler::OnFetchRecord(CassFuture *future, void *data)
{
    FetchRecordData *fetch_data = static_cast<FetchRecordData *>(data);
    txservice::FetchRecordCc *fetch_cc = fetch_data->fetch_cc_;
    const CassResult *result = nullptr;
    auto rc = cass_future_error_code(future);
    CODE_FAULT_INJECTOR("FetchRecordFail", {
        LOG(INFO) << "FaultInject  "
                     "FetchRecordFail";

        FaultInject::Instance().InjectFault("FetchRecordFail", "remove");
        rc = CASS_ERROR_SERVER_READ_TIMEOUT;
    });
    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION,
                                           fetch_cc->start_);
        metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
    }

    if (rc != CASS_OK || (result = cass_future_get_result(future)) == nullptr)
    {
        if (fetch_data->handler_->ddl_skip_kv_)
        {
            fetch_cc->rec_ts_ = 1;
            fetch_cc->rec_status_ = RecordStatus::Deleted;
            fetch_cc->SetFinish(0);
        }
        else
        {
            LOG(ERROR) << "Failed to fetch record from kv for table "
                       << fetch_cc->table_name_->Trace()
                       << ", err: " << ErrorMessage(future);
            fetch_cc->SetFinish(static_cast<int>(CcErrorCode::DATA_STORE_ERR));
        }
        return;
    }

    const CassRow *row = cass_result_first_row(result);
    if (row == NULL)
    {
        // CommitTs= 1 indicate non-existence
        fetch_cc->rec_ts_ = 1;
        fetch_cc->rec_status_ = RecordStatus::Deleted;
    }
    else
    {
        const TableName &table_name = *fetch_cc->table_name_;
        uint16_t record_col_cnt = 1;

        int64_t version_ts;
        cass_value_get_int64(cass_row_get_column(row, record_col_cnt + 2),
                             &version_ts);

        fetch_cc->rec_ts_ = version_ts;
        cass_bool_t deleted = cass_false;
        cass_value_get_bool(cass_row_get_column(row, record_col_cnt + 3),
                            &deleted);
        fetch_cc->rec_status_ =
            deleted ? RecordStatus::Deleted : RecordStatus::Normal;

        if (!deleted)
        {
            fetch_cc->table_schema_->RecordSchema()->EncodeToSerializeFormat(
                table_name, row, fetch_cc->rec_str_);
        }
    }

    fetch_cc->SetFinish(0);

    cass_result_free(result);
    delete fetch_data;
}

/**
 * @brief Read a row from base table or skindex table in Cassandra with
 * specified key. Caller should pass in complete primary key or skindex key.
 *
 * @param table_name base table name or sk index name.
 * @param key
 * @param rec
 * @param found
 * @param version_ts
 * @param table_schema
 * @param table_schema_ts
 * @return true
 * @return false
 */
bool EloqDS::CassHandler::Read(const txservice::TableName &table_name,
                               const txservice::TxKey &tx_key,
                               txservice::TxRecord &rec,
                               bool &found,
                               uint64_t &version_ts,
                               const txservice::TableSchema *table_schema)
{
    const CassPrepared *read_prepared =
        GetReadPrepared(table_name, table_schema);
    if (read_prepared == nullptr)
    {
        return false;
    }

    CassStatement *statement = cass_prepared_bind(read_prepared);
    cass_statement_set_is_idempotent(statement, cass_true);

    if (partition_finder == nullptr)
    {
        partition_finder = PartitionFinderFactory::Create();
    }
#ifdef RANGE_PARTITION_ENABLED
    if (!dynamic_cast<RangePartitionFinder *>(partition_finder.get())
             ->Init(tx_service_, UINT32_MAX))
    {
        LOG(ERROR) << "Failed to init RangePartitionFinder!";
        return false;
    }
#endif

    Partition pk;
    PartitionResultType rt =
        partition_finder->FindPartition(table_name, tx_key, pk);
    if (rt != PartitionResultType::NORMAL)
    {
        partition_finder->ReleaseReadLocks();
        return false;
    }
    int32_t pk1 = pk.Pk1();
    int16_t pk2 = pk.Pk2();
    // Bind kv table name
    cass_statement_bind_string(
        statement,
        0,
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name).data());
    // Bind pk1
    cass_statement_bind_int32(statement, 1, pk1);
    // Bind pk2
    cass_statement_bind_int16(statement, 2, pk2);

    // Binds the key field
    cass_statement_bind_bytes(statement,
                              3,
                              reinterpret_cast<const uint8_t *>(tx_key.Data()),
                              tx_key.Size());

    // set cass read start time
    metrics::TimePoint start;
    if (metrics::enable_kv_metrics)
    {
        start = metrics::Clock::now();
    }

    CassFuture *future = cass_session_execute(session_, statement);
    // Gets the error code from future. If the future is not ready this method
    // will wait for the future to be set.
    CassError ce = cass_future_error_code(future);

    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_READ_DURATION,
                                           start);
        metrics::kv_meter->Collect(metrics::NAME_KV_READ_TOTAL, 1);
    }

    if (ce != CASS_OK)
    {
        LOG(ERROR) << ErrorMessage(future);
    }
    cass_statement_free(statement);

    /* This will also block until the query returns */
    const CassResult *result = cass_future_get_result(future);
    /* The future can be freed immediately after getting the result object */
    cass_future_free(future);

    partition_finder->ReleaseReadLocks();

    /* If there was an error then the result won't be available */
    if (result == nullptr)
    {
        return false;
    }

    const CassRow *row = cass_result_first_row(result);
    if (row == NULL)
    {
        found = false;
    }
    else
    {
        found = true;

        uint16_t record_col_cnt = 1;

        int64_t *ts = reinterpret_cast<int64_t *>(&version_ts);
        cass_value_get_int64(cass_row_get_column(row, record_col_cnt + 2), ts);

        cass_bool_t deleted = cass_false;
        cass_value_get_bool(cass_row_get_column(row, record_col_cnt + 3),
                            &deleted);
        found = (deleted == cass_false);
        if (!deleted)
        {
            table_schema->RecordSchema()->EncodeToTxRecord(
                table_name, row, rec);
        }
    }

    cass_result_free(result);

    return true;
}

txservice::store::DataStoreHandler::DataStoreOpStatus
EloqDS::CassHandler::LoadRangeSlice(const txservice::TableName &table_name,
                                    const txservice::KVCatalogInfo *kv_info,
                                    uint32_t range_partition_id,
                                    txservice::FillStoreSliceCc *load_slice_req)
{
    int64_t leader_term =
        Sharder::Instance().TryPinNodeGroupData(load_slice_req->NodeGroup());
    if (leader_term < 0)
    {
        return txservice::store::DataStoreHandler::DataStoreOpStatus::Error;
    }

    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id = load_slice_req->NodeGroup()](void *)
        { Sharder::Instance().UnpinNodeGroupData(ng_id); });

    if (leader_term != load_slice_req->Term())
    {
        return txservice::store::DataStoreHandler::DataStoreOpStatus::Error;
    }

    CassPreparedType scan_type;

    if (load_slice_req->EndKey().Type() == KeyType::PositiveInf)
    {
        if (load_slice_req->SnapshotTs() == 0)
        {
            scan_type = CassPreparedType::ScanLastSlice;
        }
        else
        {
            scan_type = CassPreparedType::SnapshotScanLastSlice;
        }
    }
    else
    {
        if (load_slice_req->SnapshotTs() == 0)
        {
            scan_type = CassPreparedType::ScanSlice;
        }
        else
        {
            scan_type = CassPreparedType::SnapshotScanSlice;
        }
    }

    // for now, kv table schmea is always same. so set schema_version to 0
    const CassPrepared *scan_prepared =
        GetCachedPreparedStmt(cass_eloq_kv_table_name, 0, scan_type);

    if (scan_prepared == nullptr)
    {
        if (!CreateCachedPrepareStmt(cass_eloq_kv_table_name, 0, scan_type))
        {
            return txservice::store::DataStoreHandler::DataStoreOpStatus::Retry;
        }

        std::string scan_str("SELECT ");

        scan_str.append(" \"___encoded_blob___\", ");
        scan_str.append(
            " \"___mono_key___\", \"___unpack_info___\", "
            "\"___version___\", \"___deleted___\" FROM ");
        scan_str.append(keyspace_name_);
        scan_str.append(".");
        scan_str.append(cass_eloq_kv_table_name);
        scan_str.append(
            " WHERE kvtablename=? AND pk1_=? AND pk2_=? AND "
            "\"___mono_key___\">=?");

        // The caller of LoadRangeSlice() represents positive infinity as a null
        // pointer.
        if (scan_type == CassPreparedType::ScanSlice ||
            scan_type == CassPreparedType::SnapshotScanSlice)
        {
            scan_str.append(" AND \"___mono_key___\"<?");
        }

        if (scan_type == CassPreparedType::ScanSlice ||
            scan_type == CassPreparedType::ScanLastSlice)
        {
            scan_str.append(" AND \"___deleted___\" = false");
        }

        scan_str.append(" ALLOW FILTERING");

        ScanSliceData *scan_slice_data = new ScanSliceData(load_slice_req,
                                                           session_,
                                                           range_partition_id,
                                                           &table_name,
                                                           ddl_skip_kv_,
                                                           defer_unpin,
                                                           kv_info,
                                                           this,
                                                           scan_type);
        // LoadRangeSlice is called by tx processor, it cannot make sync data
        // store call.
        CassFuture *future = cass_session_prepare(session_, scan_str.c_str());
        cass_future_set_callback(
            future,
            [](CassFuture *future, void *data)
            {
                ScanSliceData *scan_slice_data =
                    static_cast<ScanSliceData *>(data);
                CassError rc = cass_future_error_code(future);
                if (rc != CASS_OK)
                {
                    if (scan_slice_data->ddl_skip_kv_)
                    {
                        // skip kv
                        scan_slice_data->load_slice_req_->SetKvFinish(true);
                    }
                    else
                    {
                        LOG(ERROR) << "Failed to create prepare statement for "
                                      "load range "
                                      "slice , "
                                   << ErrorMessage(future);
                        scan_slice_data->load_slice_req_->SetKvFinish(false);
                    }
                    delete scan_slice_data;
                    return;
                }
                auto prepared = cass_future_get_prepared(future);

                scan_slice_data->handler_->CachePreparedStmt(
                    cass_eloq_kv_table_name,
                    0,
                    prepared,
                    scan_slice_data->prepare_type_);
                scan_slice_data->handler_->LoadRangeSlice(
                    *scan_slice_data->table_name_,
                    scan_slice_data->kv_info_,
                    scan_slice_data->range_partition_id_,
                    scan_slice_data->load_slice_req_);
                delete scan_slice_data;
            },
            scan_slice_data);
        cass_future_free(future);
        return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
    }

    ScanSliceData *scan_slice_data = new ScanSliceData(load_slice_req,
                                                       session_,
                                                       range_partition_id,
                                                       &table_name,
                                                       ddl_skip_kv_,
                                                       defer_unpin,
                                                       kv_info,
                                                       this,
                                                       scan_type);

    CassStatement *scan_stmt = cass_prepared_bind(scan_prepared);
    cass_statement_set_is_idempotent(scan_stmt, cass_true);
    // Bind table uuid
    cass_statement_bind_string(
        scan_stmt, 0, kv_info->GetKvTableName(table_name).data());
    // Bind pk1
    cass_statement_bind_int32(scan_stmt, 1, range_partition_id);
    // Bind pk2
    cass_statement_bind_int16(scan_stmt, 2, -1);

    const TxKey &slice_start = load_slice_req->StartKey();
    const TxKey mono_start =
        (slice_start.Type() == KeyType::NegativeInf)
            ? TxKeyFactory::PackedNegativeInfinity()->GetShallowCopy()
            : slice_start.GetShallowCopy();

    cass_statement_bind_bytes(
        scan_stmt,
        3,
        reinterpret_cast<const uint8_t *>(mono_start.Data()),
        mono_start.Size());

    const TxKey &slice_end = load_slice_req->EndKey();
    if (slice_end.Type() != KeyType::PositiveInf)
    {
        cass_statement_bind_bytes(
            scan_stmt,
            4,
            reinterpret_cast<const uint8_t *>(slice_end.Data()),
            slice_end.Size());
    }

    cass_statement_set_paging_size(scan_stmt, 1000);
    scan_slice_data->scan_stmt_ = scan_stmt;
    CassFuture *scan_slice_future = cass_session_execute(session_, scan_stmt);
    cass_future_set_callback(
        scan_slice_future, OnLoadRangeSlice, scan_slice_data);

    // Cass statement will be freed when ScanSliceData is deconstructed.
    cass_future_free(scan_slice_future);

    return DataStoreOpStatus::Success;
}

bool EloqDS::CassHandler::UpdateRangeSlices(
    const txservice::TableName &table_name,
    uint64_t slice_version,
    txservice::TxKey range_start_key,
    std::vector<const txservice::StoreSlice *> slices,
    int32_t partition_id,
    uint64_t range_version)
{
    const CassPrepared *update_slice_prepared = nullptr;

    CassPreparedType prepared_type = CassPreparedType::UpdateSlice;

    // table_ranges will never have schema change so pass in 1 as schema_ts.
    update_slice_prepared =
        GetCachedPreparedStmt(cass_range_table_name, 1, prepared_type);

    if (update_slice_prepared == nullptr)
    {
        std::string cql("INSERT INTO ");
        cql.append(keyspace_name_);
        cql.append(".");
        cql.append(cass_range_table_name);

        cql.append(
            " (tablename, \"___mono_key___\", \"___segment_id___\", "
            "\"___segment_cnt___\", \"___slice_version___\", "
            "\"___slice_keys___\", \"___slice_sizes___\", "
            "\"___partition_id___\", \"___version___\") VALUES "
            "(?,?,?,?,?,?,?,?,?) USING TIMESTAMP ?");

        CassFuture *future = cass_session_prepare(session_, cql.c_str());
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            return false;
        }

        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
        {
            LOG(ERROR) << "Fail to create a prepared statement for upserting "
                          "slices. Error: "
                       << ErrorMessage(future) << " CQL: " << cql;
            cass_future_free(future);
            return false;
        }
        else
        {
            update_slice_prepared = cass_future_get_prepared(future);
            update_slice_prepared = CachePreparedStmt(
                cass_range_table_name, 1, update_slice_prepared, prepared_type);
            cass_future_free(future);
        }
    }

    TxKey mono_key =
        range_start_key.Type() == txservice::KeyType::NegativeInf
            ? TxKeyFactory::PackedNegativeInfinity()->GetShallowCopy()
            : range_start_key.GetShallowCopy();

    CassBatch *update_batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    cass_batch_set_is_idempotent(update_batch, cass_true);

    std::vector<CassStatement *> cass_stmts;

    std::string slice_size_bytes;
    std::string slice_key_bytes;

    if (slices.size() > 1)
    {
        size_t key_len = slices[1]->StartTxKey().Size();
        size_t slice_num = slices.size();

        // key_len(uint32_t) + key_data(key_len)
        size_t estimate_slice_key_bytes_len =
            slice_num * (key_len + sizeof(uint32_t));

        // 16KB
        if (estimate_slice_key_bytes_len >= 16 * 1024)
        {
            slice_key_bytes.reserve(16 * 1024);
            size_t batch_slice_num =
                ((128 * 1024) / (key_len + sizeof(uint32_t))) + 1;
            size_t estimate_slice_size_bytes_len =
                batch_slice_num * sizeof(uint32_t);

            slice_size_bytes.reserve(estimate_slice_size_bytes_len);
        }
        else
        {
            slice_key_bytes.reserve(estimate_slice_key_bytes_len);
            size_t estimate_slice_size_bytes_len = slice_num * sizeof(uint32_t);
            slice_size_bytes.reserve(estimate_slice_size_bytes_len);
        }
    }

    int64_t segment_id = 0;

    if (slices.empty())
    {
        // empty range should have a default empty slice.
        uint32_t size = 0;
        slice_size_bytes.append(reinterpret_cast<const char *>(&size),
                                sizeof(uint32_t));
    }
    else
    {
        uint32_t size = static_cast<uint32_t>(slices.at(0)->Size());
        slice_size_bytes.append(reinterpret_cast<const char *>(&size),
                                sizeof(uint32_t));
    }

    assert(table_name.Engine() != TableEngine::None);
    std::string table_key = table_name.Serialize();

    // The start key of the first slice is the same as the range's start key.
    // When storing slices' start keys, skips the first slice.
    for (size_t idx = 1; idx < slices.size(); ++idx)
    {
        const TxKey slice_key = slices[idx]->StartTxKey().GetShallowCopy();

        if (slice_key_bytes.size() + slice_key.Size() + sizeof(uint32_t) >=
            128 * 1024)
        {
            CassStatement *update_slice_stmt =
                cass_prepared_bind(update_slice_prepared);
            cass_statement_set_is_idempotent(update_slice_stmt, cass_true);

            // Bind TableName
            cass_statement_bind_string(update_slice_stmt, 0, table_key.data());
            // Bind mono_key
            cass_statement_bind_bytes(
                update_slice_stmt,
                1,
                reinterpret_cast<const uint8_t *>(slice_key.Data()),
                slice_key.Size());
            // Bind segment_id
            cass_statement_bind_int64(update_slice_stmt, 2, segment_id);

            // Bind slice_version
            cass_statement_bind_int64(update_slice_stmt, 4, slice_version);
            // Bind slice_keys
            cass_statement_bind_bytes(
                update_slice_stmt,
                5,
                reinterpret_cast<const cass_byte_t *>(slice_key_bytes.data()),
                slice_key_bytes.size());

            // Bind slice_values
            cass_statement_bind_bytes(
                update_slice_stmt,
                6,
                reinterpret_cast<const cass_byte_t *>(slice_size_bytes.data()),
                slice_size_bytes.size());

            cass_statement_bind_int32(update_slice_stmt, 7, partition_id);
            cass_statement_bind_int64(update_slice_stmt, 8, range_version);

            // Bind Using Timestamp
            // we'll use USING TIMESTAMP `slice_version` to control overwrite
            // policy
            cass_statement_bind_int64(update_slice_stmt, 9, slice_version);

            cass_stmts.push_back(update_slice_stmt);

            slice_key_bytes.clear();
            slice_size_bytes.clear();

            assert(slice_key_bytes.empty());
            assert(slice_size_bytes.empty());

            segment_id += 1;
        }

        uint32_t slice_key_size = static_cast<uint32_t>(slice_key.Size());
        slice_key_bytes.append(reinterpret_cast<const char *>(&slice_key_size),
                               sizeof(uint32_t));
        slice_key_bytes.append(slice_key.Data(), slice_key_size);

        uint32_t current_slice_size =
            static_cast<uint32_t>(slices[idx]->Size());
        slice_size_bytes.append(
            reinterpret_cast<const char *>(&current_slice_size),
            sizeof(uint32_t));
    }

    assert(slice_key_bytes.size() > 0 || slice_size_bytes.size() > 0);

    CassStatement *update_slice_stmt =
        cass_prepared_bind(update_slice_prepared);
    cass_statement_set_is_idempotent(update_slice_stmt, cass_true);
    // Bind TableName
    cass_statement_bind_string(update_slice_stmt, 0, table_key.data());
    // Bind mono_key
    cass_statement_bind_bytes(
        update_slice_stmt,
        1,
        reinterpret_cast<const uint8_t *>(mono_key.Data()),
        mono_key.Size());
    // Bind segment_id
    cass_statement_bind_int64(update_slice_stmt, 2, segment_id);
    // Bind slice_version
    cass_statement_bind_int64(update_slice_stmt, 4, slice_version);
    // Bind slice_keys
    cass_statement_bind_bytes(
        update_slice_stmt,
        5,
        reinterpret_cast<const cass_byte_t *>(slice_key_bytes.data()),
        slice_key_bytes.size());
    // Bind slice_values
    cass_statement_bind_bytes(
        update_slice_stmt,
        6,
        reinterpret_cast<const cass_byte_t *>(slice_size_bytes.data()),
        slice_size_bytes.size());

    cass_statement_bind_int32(update_slice_stmt, 7, partition_id);
    cass_statement_bind_int64(update_slice_stmt, 8, range_version);

    // we'll use USING TIMESTAMP `slice_version` to control overwrite policy
    cass_statement_bind_int64(update_slice_stmt, 9, slice_version);

    cass_stmts.push_back(update_slice_stmt);

    assert(static_cast<uint64_t>(segment_id) + 1 == cass_stmts.size());

    for (auto &stmt : cass_stmts)
    {
        // Bind segment cnt
        cass_statement_bind_int64(stmt, 3, segment_id + 1);
        CassError ce = cass_batch_add_statement(update_batch, stmt);
        assert(ce == CASS_OK);
        (void) ce;

        cass_statement_free(stmt);
    }

    CassFuture *update_slice_future =
        cass_session_execute_batch(session_, update_batch);
    /* This will block until the query returns */
    CassError rc = cass_future_error_code(update_slice_future);
    if (rc != CASS_OK)
    {
        LOG(ERROR) << "Update range slices failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(update_slice_future)
                   << ", tablename: " << table_name.StringView();
        cass_future_free(update_slice_future);
        cass_batch_free(update_batch);
        return false;
    }

    // Free future
    cass_future_free(update_slice_future);
    cass_batch_free(update_batch);

    return true;
}

std::unique_ptr<txservice::store::DataStoreScanner>
EloqDS::CassHandler::ScanForward(
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
#ifdef RANGE_PARTITION_ENABLED
    RangePartitionCassScanner *scanner =
        new RangePartitionCassScanner(session_,
                                      keyspace_name_,
                                      key_schema,
                                      rec_schema,
                                      table_name,
                                      ng_id,
                                      kv_info,
                                      start_key,
                                      inclusive,
                                      search_cond,
                                      scan_forward,
                                      tx_service_);
    scanner->MoveNext();
    return std::unique_ptr<EloqDS::CassScanner>(
        static_cast<CassScanner *>(scanner));
#else
    std::unique_ptr<CassScanner> scanner = nullptr;
    if (scan_forward)
    {
        scanner =
            std::make_unique<HashPartitionCassScanner<true>>(session_,
                                                             keyspace_name_,
                                                             key_schema,
                                                             rec_schema,
                                                             table_name,
                                                             kv_info,
                                                             start_key,
                                                             inclusive,
                                                             search_cond);
    }
    else
    {
        scanner =
            std::make_unique<HashPartitionCassScanner<false>>(session_,
                                                              keyspace_name_,
                                                              key_schema,
                                                              rec_schema,
                                                              table_name,
                                                              kv_info,
                                                              start_key,
                                                              inclusive,
                                                              search_cond);
    }

    scanner->MoveNext();
    return scanner;
#endif
}

const CassPrepared *EloqDS::CassHandler::GetCachedPreparedStmt(
    const std::string &kv_table_name,
    uint64_t table_schema_ts,
    CassPreparedType stmt_type)
{
    std::shared_lock<std::shared_mutex> lock(s_mux_);

    auto prepare_it = prepared_cache_.find(kv_table_name);
    if (prepare_it == prepared_cache_.end())
    {
        return nullptr;
    }

    CachedPrepared &prepared = prepare_it->second;
    uint64_t cached_table_schema_ts = prepared.GetTableSchemaTs();
    if (cached_table_schema_ts < table_schema_ts)
    {
        return nullptr;
    }
    else
    {
        assert(cached_table_schema_ts == table_schema_ts);
        const CassPrepared *prepared_stmt = prepared.GetPreparedStmt(stmt_type);
        return prepared_stmt;
    }
}

const CassPrepared *EloqDS::CassHandler::CachePreparedStmt(
    const std::string &kv_table_name,
    uint64_t table_schema_ts,
    const CassPrepared *prepared_stmt,
    CassPreparedType stmt_type)
{
    std::lock_guard<std::shared_mutex> lock(s_mux_);

    auto prepare_it =
        prepared_cache_.try_emplace(kv_table_name, table_schema_ts);
    CachedPrepared &cached_prepared = prepare_it.first->second;

    uint64_t cached_table_schema_ts = cached_prepared.GetTableSchemaTs();
    if (cached_table_schema_ts == table_schema_ts)
    {
        auto [cached_prepared_stmt, is_set] =
            cached_prepared.SetPreparedStmtNx(stmt_type, prepared_stmt);
        if (!is_set)
        {
            cass_prepared_free(prepared_stmt);
        }
        return cached_prepared_stmt;
    }
    else
    {
        assert(cached_table_schema_ts < table_schema_ts);
        cached_prepared.FreePrepared();
        cached_prepared.SetTableSchemaTs(table_schema_ts);
        auto [cached_prepared_stmt, is_set] =
            cached_prepared.SetPreparedStmtNx(stmt_type, prepared_stmt);
        assert(is_set);
        return cached_prepared_stmt;
    }
}

bool EloqDS::CassHandler::CreateCachedPrepareStmt(
    const std::string &kv_table_name,
    uint64_t table_schema_ts,
    CassPreparedType stmt_type)
{
    std::unique_lock<std::shared_mutex> lk(s_mux_);
    auto prepare_it =
        prepared_cache_.try_emplace(kv_table_name, table_schema_ts);
    CachedPrepared &cached_prepared = prepare_it.first->second;

    CachedPrepared::CachedPreparedStatus cached_status =
        cached_prepared.GetCachedStatus(stmt_type);
    uint64_t cached_table_schema_ts = cached_prepared.GetTableSchemaTs();
    if (cached_status == CachedPrepared::CachedPreparedStatus::BeingBuilt ||
        (cached_status == CachedPrepared::CachedPreparedStatus::Cached &&
         table_schema_ts == cached_table_schema_ts))
    {
        return false;
    }
    cached_prepared.SetCachedStatus(
        stmt_type, CachedPrepared::CachedPreparedStatus::BeingBuilt);
    return true;
}

bool EloqDS::CassHandler::FetchTable(const txservice::TableName &table_name,
                                     std::string &schema_image,
                                     bool &found,
                                     uint64_t &version_ts) const
{
    found = false;
    // fetch the table list for all the eloq tables in Cassandra.
    CassError rc = CASS_OK;
    CassStatement *statement = NULL;
    CassFuture *future = NULL;
    std::string table_key = table_name.Serialize();

    std::string query(
        "SELECT content, version, kvtablename, "
        "kvindexname, keyschemasts FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_table_catalog_name);
    query.append(" WHERE tablename='");
    query.append(table_key);
    query.append("'");

    statement = cass_statement_new(query.c_str(), 0);
    future = cass_session_execute(session_, statement);
    if (!cass_future_wait_timed(future, future_wait_timeout))
    {
        cass_future_free(future);
        cass_statement_free(statement);
        return false;
    }

    rc = cass_future_error_code(future);
    bool ok = (rc == CASS_OK);
    if (ok)
    {
        const CassResult *result = cass_future_get_result(future);
        const CassRow *row = cass_result_first_row(result);
        if (row != nullptr)
        {
            const char *item;
            size_t item_length;

            cass_value_get_string(
                cass_row_get_column(row, 0), &item, &item_length);
            std::string frm(item, item_length);
            int64_t *ts = reinterpret_cast<int64_t *>(&version_ts);
            cass_value_get_int64(cass_row_get_column(row, 1), ts);
            cass_value_get_string(
                cass_row_get_column(row, 2), &item, &item_length);
            std::string kv_table_name(item, item_length);
            cass_value_get_string(
                cass_row_get_column(row, 3), &item, &item_length);
            std::string kv_index_name(item, item_length);
            cass_value_get_string(
                cass_row_get_column(row, 4), &item, &item_length);
            std::string key_schemas_ts(item, item_length);
            schema_image.append(SerializeSchemaImage(
                frm,
                CassCatalogInfo(kv_table_name, kv_index_name).Serialize(),
                TableKeySchemaTs(key_schemas_ts, table_name.Engine())
                    .Serialize()));
            assert(!schema_image.empty());
            found = true;
        }
        else
        {
            // version_ts= 1 indicate non-existence
            version_ts = 1;
            schema_image.clear();
            found = false;
        }

        cass_result_free(result);
    }
    else
    {
        LOG(ERROR) << "Fetch table from cassandra failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(future)
                   << "table name: " << table_name.StringView();
    }

    cass_future_free(future);
    cass_statement_free(statement);

    return ok;
}

/**
 * @brief Discovery the eloq table names in data store.
 */
bool EloqDS::CassHandler::DiscoverAllTableNames(
    std::vector<std::string> &norm_name_vec,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr) const
{
    // discovery all the table names in Cassandra.
    CassStatement *statement = NULL;
    CassFuture *future = NULL;

    // TODO: add engine to table name
    std::string query("SELECT tablename FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_table_catalog_name);
    statement = cass_statement_new(query.c_str(), 0);

    future = cass_session_execute(session_, statement);
    if (yield_fptr)
    {
        cass_future_set_callback(
            future,
            [](CassFuture *future, void *data)
            {
                auto resume_fptr = static_cast<std::function<void()> *>(data);
                (*resume_fptr)();
            },
            (void *) resume_fptr);
        (*yield_fptr)();
    }
    else
    {
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            cass_statement_free(statement);
            return false;
        }
    }

    CassError rc = cass_future_error_code(future);

    bool ok = (rc == CASS_OK);
    if (!ok)
    {
        LOG(ERROR) << "Discover all tablenames from cassandra failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(future);
    }
    else
    {
        const CassResult *result = cass_future_get_result(future);
        CassIterator *iterator = cass_iterator_from_result(result);

        while (cass_iterator_next(iterator))
        {
            const char *item;
            size_t item_length;
            const CassRow *row = cass_iterator_get_row(iterator);
            cass_value_get_string(
                cass_row_get_column(row, 0), &item, &item_length);
            // engine type
            assert(item_length >= sizeof(int8_t));
            std::string tablename(item, item_length - sizeof(int8_t));
            norm_name_vec.push_back(std::move(tablename));
        }

        cass_result_free(result);
        cass_iterator_free(iterator);
    }

    cass_future_free(future);
    cass_statement_free(statement);

    return ok;
}

bool EloqDS::CassHandler::UpsertDatabase(std::string_view db,
                                         std::string_view definition) const
{
    std::string query("INSERT INTO ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_database_catalog_name);
    query.append(" (dbname, definition) VALUES (?, ?)");

    CassStatement *stmt = cass_statement_new(query.c_str(), 2);

    cass_statement_bind_string_n(stmt, 0, db.data(), db.length());
    cass_statement_bind_bytes(
        stmt,
        1,
        reinterpret_cast<const cass_byte_t *>(definition.data()),
        definition.size());

    CassFuture *future = cass_session_execute(session_, stmt);

    CassError rc = cass_future_error_code(future);

    bool ok = (rc == CASS_OK);
    if (!ok)
    {
        LOG(ERROR) << "Upsert database from cassandra failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(future) << ", "
                   << "db: " << db;
    }

    cass_future_free(future);
    cass_statement_free(stmt);

    return ok;
}

bool EloqDS::CassHandler::DropDatabase(std::string_view db) const
{
    std::string query("DELETE FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_database_catalog_name);
    query.append(" WHERE dbname = ?");
    CassStatement *stmt = cass_statement_new(query.c_str(), 1);

    cass_statement_bind_string_n(stmt, 0, db.data(), db.length());

    CassFuture *future = cass_session_execute(session_, stmt);

    CassError rc = cass_future_error_code(future);

    bool ok = (rc == CASS_OK);
    if (!ok)
    {
        LOG(ERROR) << "Drop database from cassandra failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(future) << ", "
                   << "db: " << db;
    }

    cass_future_free(future);
    cass_statement_free(stmt);

    return ok;
}

bool EloqDS::CassHandler::FetchDatabase(
    std::string_view db,
    std::string &definition,
    bool &found,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr) const
{
    std::string query("SELECT * FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_database_catalog_name);
    query.append(" WHERE dbname = ?");
    CassStatement *stmt = cass_statement_new(query.c_str(), 1);

    cass_statement_bind_string_n(stmt, 0, db.data(), db.length());

    CassFuture *future = cass_session_execute(session_, stmt);
    if (yield_fptr)
    {
        cass_future_set_callback(
            future,
            [](CassFuture *future, void *data)
            {
                auto resume_fptr = static_cast<std::function<void()> *>(data);
                (*resume_fptr)();
            },
            (void *) resume_fptr);
        (*yield_fptr)();
    }
    else
    {
        cass_future_wait(future);
    }

    CassError rc = cass_future_error_code(future);

    bool ok = (rc == CASS_OK);
    if (!ok)
    {
        LOG(ERROR) << "Fetch database from cassandra failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(future) << ", "
                   << "db: " << db;
    }
    else
    {
        const CassResult *result = cass_future_get_result(future);

        const CassRow *row = cass_result_first_row(result);

        if (row)
        {
            std::string col_name("definition");
            const cass_byte_t *col;
            size_t col_size;

            cass_value_get_bytes(cass_row_get_column_by_name_n(
                                     row, col_name.data(), col_name.length()),
                                 &col,
                                 &col_size);

            definition.assign(reinterpret_cast<const char *>(col), col_size);

            found = true;
        }
        else
        {
            found = false;
        }

        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);

    return ok;
}

bool EloqDS::CassHandler::FetchAllDatabase(
    std::vector<std::string> &dbnames,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr) const
{
    std::string query("SELECT * FROM ");
    query.append(keyspace_name_);
    query.append(".");
    query.append(cass_database_catalog_name);
    CassStatement *stmt = cass_statement_new(query.c_str(), 0);

    CassFuture *future = cass_session_execute(session_, stmt);
    if (!yield_fptr)
    {
        cass_future_wait(future);
    }
    else
    {
        cass_future_set_callback(
            future,
            [](CassFuture *future, void *data)
            {
                auto resume_fptr = static_cast<std::function<void()> *>(data);
                (*resume_fptr)();
            },
            (void *) resume_fptr);
        (*yield_fptr)();
    }

    CassError rc = cass_future_error_code(future);
    bool ok = (rc == CASS_OK);
    if (!ok)
    {
        LOG(ERROR) << "Fetch all database from cassandra failed, "
                   << "error code: " << rc << ", "
                   << "error message: " << ErrorMessage(future);
    }
    else
    {
        const CassResult *result = cass_future_get_result(future);

        CassIterator *iter = cass_iterator_from_result(result);

        while (cass_iterator_next(iter))
        {
            const CassRow *row = cass_iterator_get_row(iter);

            std::string col_name("dbname");
            const cass_byte_t *col;
            size_t col_size;

            cass_value_get_bytes(cass_row_get_column_by_name_n(
                                     row, col_name.data(), col_name.length()),
                                 &col,
                                 &col_size);

            dbnames.emplace_back(reinterpret_cast<const char *>(col), col_size);
        }

        cass_result_free(result);
        cass_iterator_free(iter);
    }

    cass_future_free(future);
    cass_statement_free(stmt);

    return ok;
}

bool EloqDS::CassHandler::DropKvTable(const std::string &kv_table_name) const
{
    assert(cass_sys_tables.find(kv_table_name) == cass_sys_tables.end());
    if (cass_sys_tables.find(kv_table_name) != cass_sys_tables.end())
    {
        LOG(ERROR)
            << "InternalError: Try to drop system table !!! kv table name: "
            << kv_table_name;
        return false;
    }

    std::string drop_str("DROP TABLE IF EXISTS ");
    drop_str.append(keyspace_name_);
    drop_str.append(".");
    drop_str.append(kv_table_name);
    CassStatement *drop_stmt = cass_statement_new(drop_str.c_str(), 0);
    CassFuture *cass_future = cass_session_execute(session_, drop_stmt);
    if (!cass_future_wait_timed(cass_future, future_wait_timeout))
    {
        cass_future_free(cass_future);
        cass_statement_free(drop_stmt);
        return false;
    }
    CassError ret = cass_future_error_code(cass_future);
    bool ok = ret == CASS_OK;
    if (!ok)
    {
        LOG(ERROR) << "Drop kvtable failed, kvtablename: " << kv_table_name
                   << ", " << ErrorMessage(cass_future);
    }

    cass_future_free(cass_future);
    cass_statement_free(drop_stmt);
    return ok;
}

void EloqDS::CassHandler::DeleteDataFromKvTable(const TableName *table_name,
                                                void *table_data)
{
    UpsertTableData *upsert_table_data =
        static_cast<UpsertTableData *>(table_data);

    assert(upsert_table_data->op_type_ == OperationType::DropTable ||
           upsert_table_data->op_type_ == OperationType::TruncateTable ||
           upsert_table_data->op_type_ == OperationType::DropIndex);
    const std::string &physical_table_name =
        (upsert_table_data->op_type_ == OperationType::DropTable ||
         upsert_table_data->op_type_ == OperationType::TruncateTable)
            ? upsert_table_data->schema_->GetKVCatalogInfo()->GetKvTableName(
                  *table_name)
            : (upsert_table_data->drop_indexes_it_->second);

    DropTableData *drop_table_data =
        new DropTableData(table_name, physical_table_name, upsert_table_data);
    assert(drop_table_data->range_id_vec_idx_ == 0);

    std::optional<std::vector<uint32_t>> table_range_ids;
#ifdef RANGE_PARTITION_ENABLED
    const TableName range_table_name{
        table_name->String(), TableType::RangePartition, table_name->Engine()};
    table_range_ids = Sharder::Instance().GetLocalCcShards()->GetTableRangeIds(
        range_table_name, upsert_table_data->node_group_id_);
#else
    std::vector<uint32_t> partition_ids(HashPartitionFinder::MaxPartitionCount);
    std::iota(partition_ids.begin(), partition_ids.end(), 0);
    table_range_ids = std::move(partition_ids);
    assert(table_range_ids.has_value());
#endif

    if (table_range_ids.has_value())
    {
        drop_table_data->table_range_ids_ = std::move(table_range_ids.value());
        OnDeleteDataFromKvTable(drop_table_data);
    }
    else
    {
        assert(table_name->Engine() != TableEngine::None);
        std::string table_key = table_name->Serialize();

        std::string query("SELECT \"___partition_id___\" FROM ");
        query.append(drop_table_data->table_data_->cass_hd_->keyspace_name_);
        query.append(".");
        query.append(cass_range_table_name);
        query.append(
            " WHERE tablename=? AND \"___segment_id___\" = 0 ALLOW FILTERING");

        CassStatement *fetch_stmt = cass_statement_new(query.c_str(), 1);
        // Bind table name
        cass_statement_bind_string(fetch_stmt, 0, table_key.data());
        cass_statement_set_paging_size(fetch_stmt, 50);
        cass_statement_set_is_idempotent(fetch_stmt, cass_true);

        drop_table_data->fetch_table_range_id_stmt_ = fetch_stmt;

        CassFuture *fetch_future = cass_session_execute(
            drop_table_data->table_data_->session_, fetch_stmt);
        cass_future_set_callback(
            fetch_future, OnFetchTableRangeId, drop_table_data);
        cass_future_free(fetch_future);
    }
}

void EloqDS::CassHandler::OnFetchTableRangeId(CassFuture *future, void *data)
{
    DropTableData *drop_table_data = static_cast<DropTableData *>(data);
    CassError code = cass_future_error_code(future);
    if (code != CASS_OK)
    {
        LOG(ERROR) << "DropTable: Fetch table range id failed: "
                   << "tablename: "
                   << drop_table_data->local_table_name_->StringView()
                   << "err msg: " << ErrorMessage(future);
        // Free statement
        cass_statement_free(drop_table_data->fetch_table_range_id_stmt_);

        if (drop_table_data->table_data_->ref_count_.fetch_sub(1) == 1)
        {
            drop_table_data->table_data_->hd_res_->SetError(
                CcErrorCode::DATA_STORE_ERR);
            // Release object
            delete drop_table_data->table_data_;
        }
        else
        {
            drop_table_data->table_data_->SetErrorCode(
                CcErrorCode::DATA_STORE_ERR);
        }

        delete drop_table_data;
        return;
    }

    const CassResult *result = cass_future_get_result(future);
    CassIterator *iter = cass_iterator_from_result(result);

    while (cass_iterator_next(iter) == cass_bool_t::cass_true)
    {
        const CassRow *row = cass_iterator_get_row(iter);
        const CassValue *pt_cass_val =
            cass_row_get_column_by_name(row, "___partition_id___");
        cass_int32_t partition_id = 0;
        if (pt_cass_val != nullptr && !cass_value_is_null(pt_cass_val))
        {
            cass_value_get_int32(pt_cass_val, &partition_id);
            drop_table_data->table_range_ids_.push_back(partition_id);
        }
    }

    if (cass_result_has_more_pages(result))
    {
        cass_statement_set_paging_state(
            drop_table_data->fetch_table_range_id_stmt_, result);
        CassFuture *future =
            cass_session_execute(drop_table_data->table_data_->session_,
                                 drop_table_data->fetch_table_range_id_stmt_);
        cass_future_set_callback(future, OnFetchTableRangeId, drop_table_data);
        cass_future_free(future);
        return;
    }

    cass_statement_free(drop_table_data->fetch_table_range_id_stmt_);

    // When ddl_skip_kv_ is enabled and the range entry is not physically
    // ready, initializes the original range from negative infinity to
    // positive infinity.
    if (drop_table_data->table_range_ids_.empty())
    {
        drop_table_data->table_range_ids_.push_back(
            Partition::InitialPartitionId(
                drop_table_data->local_table_name_->StringView()));
    }

    OnDeleteDataFromKvTable(drop_table_data);
}

void EloqDS::CassHandler::OnDeleteRangesFromKvTable(CassFuture *future,
                                                    void *table_data)
{
    DropTableData *drop_table_data = static_cast<DropTableData *>(table_data);
    CassError code = cass_future_error_code(future);
    if (code != CASS_OK)
    {
        LOG(ERROR) << "DropTable: delete data from kvtable failed: "
                   << "tablename: "
                   << drop_table_data->local_table_name_->StringView()
                   << ", err msg: " << ErrorMessage(future);

        if (drop_table_data->table_data_->ref_count_.fetch_sub(1) == 1)
        {
            drop_table_data->table_data_->hd_res_->SetError(
                CcErrorCode::DATA_STORE_ERR);
            // Release object
            delete drop_table_data->table_data_;
        }
        else
        {
            drop_table_data->table_data_->SetErrorCode(
                CcErrorCode::DATA_STORE_ERR);
        }

        delete drop_table_data;
        return;
    }

    if (drop_table_data->range_id_vec_idx_ <
        drop_table_data->table_range_ids_.size())
    {
        OnDeleteDataFromKvTable(drop_table_data);
    }
    else
    {
        assert(drop_table_data->range_id_vec_idx_ ==
               drop_table_data->table_range_ids_.size());
        UpsertTableData *upsert_table_data = drop_table_data->table_data_;
        delete drop_table_data;
        OnUpsertCassTable(nullptr, upsert_table_data);
    }
}

void EloqDS::CassHandler::OnDeleteDataFromKvTable(void *table_data)
{
    DropTableData *drop_table_data = static_cast<DropTableData *>(table_data);
    assert(!drop_table_data->table_range_ids_.empty());

    std::string delete_str("DELETE FROM ");
    delete_str.append(drop_table_data->table_data_->cass_hd_->keyspace_name_);
    delete_str.append(".");
    delete_str.append(cass_eloq_kv_table_name);
    delete_str.append("  USING TIMESTAMP ");
    delete_str.append(
        std::to_string(drop_table_data->table_data_->write_time_));
    delete_str.append(" WHERE kvtablename = ? AND pk1_ = ? AND pk2_ = ?");

    CassBatch *delete_batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    cass_batch_set_is_idempotent(delete_batch, cass_true);

    size_t end_idx = std::min(drop_table_data->table_range_ids_.size(),
                              drop_table_data->range_id_vec_idx_ + 10);
    assert(end_idx <= drop_table_data->table_range_ids_.size());

    for (; drop_table_data->range_id_vec_idx_ < end_idx;
         ++drop_table_data->range_id_vec_idx_)
    {
        CassStatement *delete_stmt = cass_statement_new(delete_str.c_str(), 3);
        cass_statement_set_is_idempotent(delete_stmt, cass_true);
        // Bind table uuid
        cass_statement_bind_string(
            delete_stmt, 0, drop_table_data->physical_table_name_.data());
        // Bind pk1_
        cass_statement_bind_int32(
            delete_stmt,
            1,
            drop_table_data
                ->table_range_ids_[drop_table_data->range_id_vec_idx_]);
        // Bind pk2_
        cass_statement_bind_int16(delete_stmt, 2, -1);
        CassError ce = cass_batch_add_statement(delete_batch, delete_stmt);
        assert(ce == CASS_OK);
        (void) ce;
        cass_statement_free(delete_stmt);
    }

    CassFuture *delete_future = cass_session_execute_batch(
        drop_table_data->table_data_->session_, delete_batch);
    cass_future_set_callback(
        delete_future, OnDeleteRangesFromKvTable, drop_table_data);
    cass_future_free(delete_future);
    cass_batch_free(delete_batch);
}

void EloqDS::CassHandler::DropKvTableAsync(
    const std::string &kv_table_name) const
{
    assert(cass_sys_tables.find(kv_table_name) == cass_sys_tables.end());
    if (cass_sys_tables.find(kv_table_name) != cass_sys_tables.end())
    {
        LOG(ERROR)
            << "!!! InternalError: Try to drop system table, kv table name: "
            << kv_table_name;
        return;
    }
    // When ddl_skip_kv is set and is_bootstrap is not set, Cassandra won't
    // create table.
    std::string drop_str("DROP TABLE IF EXISTS ");
    drop_str.append(keyspace_name_);
    drop_str.append(".");
    drop_str.append(kv_table_name);
    CassStatement *drop_stmt = cass_statement_new(drop_str.c_str(), 0);
    CassFuture *cass_future = cass_session_execute(session_, drop_stmt);
    cass_future_free(cass_future);  // ignore error
    cass_statement_free(drop_stmt);
}

std::string_view EloqDS::CassHandler::ErrorMessage(CassFuture *future)
{
    const char *message;
    size_t length;
    cass_future_error_message(future, &message, &length);
    return {message, length};
}

/**
 * @brief Create archives table in cassandra , table struct:
  { table_name text,
    key blob,
    commit_ts bigint,
    payload_status int,
    payload blob,
    PRIMARY KEY (table_name, key, commit_ts))
    WITH CLUSTERING ORDER BY (key ASC, commit_ts DESC)
  }
 *
 * @return bool
 */
bool EloqDS::CassHandler::CreateMvccArchivesTable()
{
    // create keyspace if not exists in Cassandra.
    CassStatement *create_statement = NULL;
    CassFuture *create_future = NULL;
    std::string mvcc_table_name;
    mvcc_table_name.append(keyspace_name_);
    mvcc_table_name.append(".");
    mvcc_table_name.append(cass_mvcc_archive_name);
    std::string ct_query = "CREATE TABLE IF NOT EXISTS ";
    ct_query.append(mvcc_table_name);
    ct_query.append(
        "(kv_table_name text, mono_key blob, commit_ts bigint, "
        "deleted boolean, payload blob, unpack_info blob, "
        "PRIMARY KEY ((kv_table_name, mono_key), commit_ts)) WITH CLUSTERING "
        "ORDER BY (commit_ts DESC)");

    DLOG(INFO) << "CreateMvccArchivesTable: " << ct_query;

    create_statement = cass_statement_new(ct_query.c_str(), 0);
    create_future = cass_session_execute(session_, create_statement);
    if (!cass_future_wait_timed(create_future, future_wait_timeout))
    {
        cass_future_free(create_future);
        cass_statement_free(create_statement);
        return false;
    }
    CassError rc = cass_future_error_code(create_future);

    cass_future_free(create_future);
    cass_statement_free(create_statement);

    if (rc == CASS_OK)
    {
        DLOG(INFO) << "CreateMvccArchivesTable succeed.";
    }
    else
    {
        DLOG(INFO) << "CreateMvccArchivesTable fail !!! "
                   << ErrorMessage(create_future);
    }

    return (rc == CASS_OK);
}

void EloqDS::CassHandler::DecodeArchiveRowFromCassRow(
    const txservice::TableName &table_name,
    const CassRow *row,
    TxRecord &payload,
    txservice::RecordStatus &payload_status,
    uint64_t &commit_ts)
{
    const CassValue *cass_val = cass_row_get_column_by_name(row, "commit_ts");
    int64_t tmp_ts = 0;
    CassError res = cass_value_get_int64(cass_val, &tmp_ts);
    assert(res == CassError::CASS_OK);
    (void) res;
    assert(tmp_ts >= 0);
    commit_ts = static_cast<uint64_t>(tmp_ts);

    cass_val = cass_row_get_column_by_name(row, "deleted");
    cass_bool_t deleted = cass_false;
    res = cass_value_get_bool(cass_val, &deleted);
    assert(res == CassError::CASS_OK);
    if (deleted == cass_false)
    {
        payload_status = txservice::RecordStatus::Normal;
    }
    else
    {
        payload_status = txservice::RecordStatus::Deleted;
    }

    if (deleted == cass_false)
    {
        if (table_name.Type() == txservice::TableType::Primary ||
            table_name.Type() == txservice::TableType::UniqueSecondary)
        {
            const cass_byte_t *blob_ptr = nullptr;
            size_t blob_len = 0;
            const CassValue *cass_val =
                cass_row_get_column_by_name(row, "payload");
            CassError res =
                cass_value_get_bytes(cass_val, &blob_ptr, &blob_len);
            payload.SetEncodedBlob(blob_ptr, blob_len);
            assert(res == CassError::CASS_OK);
            (void) res;
        }

        const CassValue *unpack_info_value =
            cass_row_get_column_by_name(row, "unpack_info");
        const cass_byte_t *unpack_info = NULL;
        size_t unpack_len = 0;
        cass_value_get_bytes(unpack_info_value, &unpack_info, &unpack_len);
        payload.SetUnpackInfo(unpack_info, unpack_len);
    }
}

bool EloqDS::CassHandler::FetchVisibleArchive(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    const txservice::TxKey &key,
    const uint64_t upper_bound_ts,
    txservice::TxRecord &rec,
    txservice::RecordStatus &rec_status,
    uint64_t &commit_ts)
{
    std::string mvcc_table_name;
    mvcc_table_name.append(keyspace_name_);
    mvcc_table_name.append(".");
    mvcc_table_name.append(cass_mvcc_archive_name);
    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    const CassPrepared *read_prepared =
        GetCachedPreparedStmt(mvcc_table_name, 0U, CassPreparedType::Read);
    if (read_prepared == nullptr)
    {
        std::string read_str(
            "SELECT kv_table_name, mono_key, commit_ts, "
            "deleted, payload, unpack_info FROM ");
        read_str.append(mvcc_table_name);
        read_str.append(
            " WHERE kv_table_name=? AND mono_key=? AND commit_ts<=? LIMIT 1");

        CassFuture *future = cass_session_prepare(session_, read_str.c_str());
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            return false;
        }

        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
        {
            LOG(ERROR) << ErrorMessage(future);
            cass_future_free(future);
            return false;
        }
        else
        {
            read_prepared = cass_future_get_prepared(future);
            read_prepared = CachePreparedStmt(
                mvcc_table_name, 0U, read_prepared, CassPreparedType::Read);
            cass_future_free(future);
        }
    }

    CassStatement *statement = cass_prepared_bind(read_prepared);
    cass_statement_set_is_idempotent(statement, cass_true);

    // Bind table name
    cass_statement_bind_string_n(
        statement, 0, kv_table_name.data(), kv_table_name.size());

    // Binds the key fields.
    cass_statement_bind_bytes(statement,
                              1,
                              reinterpret_cast<const uint8_t *>(key.Data()),
                              key.Size());

    cass_statement_bind_int64(
        statement, 2, static_cast<int64_t>(upper_bound_ts));
    CassFuture *future = cass_session_execute(session_, statement);
    cass_statement_free(statement);

    /* This will also block until the query returns */
    const CassResult *result = cass_future_get_result(future);
    /* The future can be freed immediately after getting the result object */
    cass_future_free(future);

    /* If there was an error then the result won't be available */
    if (result == nullptr)
    {
        return false;
    }

    const CassRow *row = cass_result_first_row(result);
    if (row == NULL)
    {
        rec_status = txservice::RecordStatus::Deleted;
        cass_result_free(result);
        return true;
    }
    else
    {
        DecodeArchiveRowFromCassRow(
            table_name, row, rec, rec_status, commit_ts);
        cass_result_free(result);
        return true;
    }
}

bool EloqDS::CassHandler::FetchArchives(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    const txservice::TxKey &key,
    std::vector<txservice::VersionTxRecord> &archives,
    uint64_t from_ts)
{
    std::string mvcc_table_name;
    mvcc_table_name.append(keyspace_name_);
    mvcc_table_name.append(".");
    mvcc_table_name.append(cass_mvcc_archive_name);
    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    const CassPrepared *read_prepared = nullptr;

    std::string read_str(
        "SELECT kv_table_name, mono_key, commit_ts, deleted, "
        "payload, unpack_info FROM ");
    read_str.append(mvcc_table_name);
    read_str.append(" WHERE kv_table_name=? AND mono_key=? AND commit_ts>=?");

    CassFuture *future = cass_session_prepare(session_, read_str.c_str());
    if (!cass_future_wait_timed(future, future_wait_timeout))
    {
        cass_future_free(future);
        return false;
    }

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK)
    {
        cass_future_free(future);
        return false;
    }
    else
    {
        read_prepared = cass_future_get_prepared(future);
        cass_future_free(future);
    }

    CassStatement *statement = cass_prepared_bind(read_prepared);
    cass_statement_set_is_idempotent(statement, cass_true);

    // Bind table name
    cass_statement_bind_string_n(
        statement, 0, kv_table_name.data(), kv_table_name.size());

    // Binds the key fields.
    std::string key_str;
    key.Serialize(key_str);
    const uint8_t *content = reinterpret_cast<const uint8_t *>(key_str.data());
    cass_statement_bind_bytes(statement, 1, content, key_str.size());
    cass_statement_bind_int64(statement, 2, static_cast<int64_t>(from_ts));

    future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_prepared_free(read_prepared);

    /* This will also block until the query returns */
    const CassResult *result = cass_future_get_result(future);
    /* The future can be freed immediately after getting the result object */
    cass_future_free(future);

    /* If there was an error then the result won't be available */
    if (result == nullptr)
    {
        return false;
    }

    CassIterator *iterator = cass_iterator_from_result(result);
    while (cass_iterator_next(iterator))
    {
        const CassRow *row = cass_iterator_get_row(iterator);

        txservice::VersionTxRecord &akv_rec = archives.emplace_back();
        std::unique_ptr<TxRecord> tmp_rec = TxRecordFactory::CreateTxRecord();
        DecodeArchiveRowFromCassRow(table_name,
                                    row,
                                    *(tmp_rec),
                                    akv_rec.record_status_,
                                    akv_rec.commit_ts_);
        akv_rec.record_ = std::move(tmp_rec);
    }

    cass_result_free(result);
    cass_iterator_free(iterator);

    return true;
}

CassStatement *EloqDS::CassHandler::BuildStatement(CassSession *cass_session,
                                                   const std::string &stmt_str)
{
    // TODO(githubzilla): use prepared statement cache
    CassFuture *stmt_prepared_future =
        cass_session_prepare(cass_session, stmt_str.c_str());
    if (!cass_future_wait_timed(stmt_prepared_future, future_wait_timeout))
    {
        cass_future_free(stmt_prepared_future);
        return nullptr;
    }
    CassError rc = cass_future_error_code(stmt_prepared_future);
    if (rc != CASS_OK)
    {
        // Print error sql for notice
        const char *error_message;
        size_t error_message_length;
        cass_future_error_message(
            stmt_prepared_future, &error_message, &error_message_length);
        LOG(ERROR) << "Cql: " << stmt_str;
        LOG(ERROR) << "Error: " << error_message;
        cass_future_free(stmt_prepared_future);
        return nullptr;
    }

    const CassPrepared *stmt_prepared =
        cass_future_get_prepared(stmt_prepared_future);
    cass_future_free(stmt_prepared_future);

    CassStatement *stmt = cass_prepared_bind(stmt_prepared);
    cass_prepared_free(stmt_prepared);

    return stmt;
}

std::pair<const CassResult *, CassStatement *>
EloqDS::CassHandler::ExecuteStatement(
    CassSession *cass_session,
    const std::string &stmt_str,
    std::function<void(CassStatement *)> stmt_setup,
    bool return_cass_stmt)
{
    CassStatement *stmt = BuildStatement(cass_session, stmt_str);
    if (stmt == nullptr)
    {
        return std::make_pair(nullptr, nullptr);
    }
    cass_statement_set_is_idempotent(stmt, cass_true);
    if (stmt_setup != nullptr)
    {
        stmt_setup(stmt);
    }

    CassFuture *stmt_exe_future = cass_session_execute(cass_session, stmt);
    CassError rc = cass_future_error_code(stmt_exe_future);
    if (rc != CASS_OK)
    {
        cass_statement_free(stmt);
        // Print error sql for notice
        const char *error_message;
        size_t error_message_length;
        cass_future_error_message(
            stmt_exe_future, &error_message, &error_message_length);
        LOG(ERROR) << "Cql: " << stmt_str;
        LOG(ERROR) << "Error: " << error_message;
        cass_future_free(stmt_exe_future);
        return std::make_pair(nullptr, nullptr);
    }

    if (!return_cass_stmt)
    {
        cass_statement_free(stmt);
        stmt = nullptr;
    }

    const CassResult *result = cass_future_get_result(stmt_exe_future);
    cass_future_free(stmt_exe_future);

    if (result == nullptr)
    {
        cass_statement_free(stmt);
        return std::make_pair(nullptr, nullptr);
    }

    return std::make_pair(result, stmt);
}

bool EloqDS::CassHandler::ExecuteSelectStatement(
    CassSession *cass_session,
    const std::string &stmt_str,
    std::function<void(CassStatement *)> stmt_setup,
    std::function<bool(const CassRow *)> receive_row,
    int page_size)
{
    std::pair<const CassResult *, CassStatement *> res = ExecuteStatement(
        cass_session,
        stmt_str,
        [stmt_setup, page_size](CassStatement *stmt)
        {
            stmt_setup(stmt);
            cass_statement_set_paging_size(stmt, page_size);
        },
        true);

    const CassResult *result = res.first;
    CassStatement *stmt = res.second;

    if (result == nullptr)
    {
        return false;
    }

    bool has_more_data = true;
    CassIterator *it = cass_iterator_from_result(result);
    if (it == nullptr)
    {
        return false;
    }
    while (has_more_data)
    {
        if (cass_iterator_next(it))
        {
            const CassRow *row = cass_iterator_get_row(it);
            bool receive_more = receive_row(row);
            if (!receive_more)
            {
                cass_iterator_free(it);
                it = nullptr;
                cass_result_free(result);
                result = nullptr;
                cass_statement_free(stmt);
                stmt = nullptr;
                has_more_data = false;
            }
        }
        else
        {
            cass_iterator_free(it);
            it = nullptr;
            if (cass_result_has_more_pages(result))
            {
                cass_statement_set_paging_state(stmt, result);
                cass_result_free(result);
                result = nullptr;
                CassFuture *result_future =
                    cass_session_execute(cass_session, stmt);

                CassError rc = cass_future_error_code(result_future);
                if (rc != CASS_OK)
                {
                    while (rc == CASS_ERROR_SERVER_READ_TIMEOUT)
                    {
                        result_future =
                            cass_session_execute(cass_session, stmt);
                        rc = cass_future_error_code(result_future);
                    }
                    cass_statement_free(stmt);
                    // Print error sql for notice
                    const char *error_message;
                    size_t error_message_length;
                    cass_future_error_message(
                        result_future, &error_message, &error_message_length);
                    LOG(ERROR) << "Cql: " << stmt_str;
                    LOG(ERROR) << "Error: " << error_message;
                    cass_future_free(result_future);
                    return false;
                }

                result = cass_future_get_result(result_future);
                if (result == nullptr)
                {
                    cass_statement_free(stmt);
                    stmt = nullptr;
                    has_more_data = false;
                }
                else
                {
                    it = cass_iterator_from_result(result);
                    has_more_data = true;
                }
                cass_future_free(result_future);
                result_future = nullptr;
            }
            else
            {
                cass_result_free(result);
                result = nullptr;
                cass_statement_free(stmt);
                stmt = nullptr;
                has_more_data = false;
            }
        }
    }

    return true;
}

bool EloqDS::CassHandler::UpsertRanges(const txservice::TableName &table_name,
                                       std::vector<SplitRangeInfo> range_info,
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

    return true;
}

bool EloqDS::CassHandler::DeleteOutOfRangeDataInternal(
    const TableSchema *table_schema,
    const TableName &table_name,
    std::string delete_from_partition_sql,
    int32_t partition_id,
    const TxKey *start_k)
{
    auto rs = ExecuteStatement(
        session_,
        delete_from_partition_sql,
        [&table_schema, &table_name, &partition_id, start_k](
            CassStatement *stmt)
        {
            // Bind table uuid
            cass_statement_bind_string(stmt,
                                       0,
                                       table_schema->GetKVCatalogInfo()
                                           ->GetKvTableName(table_name)
                                           .data());
            // Bind pk1_
            cass_statement_bind_int32(stmt, 1, partition_id);
            // Bind pk2_
            cass_statement_bind_int16(stmt, 2, -1);
            cass_statement_bind_bytes(
                stmt,
                3,
                reinterpret_cast<const cass_byte_t *>(start_k->Data()),
                start_k->Size());
        },
        false);
    const CassResult *result = rs.first;
    if (result == nullptr)
    {
        return false;
    }
    cass_result_free(result);
    return true;
}

bool EloqDS::CassHandler::DeleteOutOfRangeData(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const txservice::TxKey *start_key,
    const txservice::TableSchema *table_schema)
{
    // Delete from
    std::string delete_from_partition_str("DELETE FROM ");
    delete_from_partition_str.append(keyspace_name_);
    delete_from_partition_str.append(".");
    delete_from_partition_str.append(cass_eloq_kv_table_name);
    // delete_from_partition_str.append(" USING TIMESTAMP ? ");
    delete_from_partition_str.append(
        " WHERE kvtablename = ? AND pk1_=? AND pk2_=? ");
    // great than start_key
    delete_from_partition_str.append(" AND \"___mono_key___\" >= ?");

    DeleteOutOfRangeDataInternal(table_schema,
                                 table_name,
                                 delete_from_partition_str,
                                 partition_id,
                                 start_key);
    return true;
}

bool EloqDS::CassHandler::GetNextRangePartitionId(
    const txservice::TableName &table_name,
    uint32_t range_cnt,
    int32_t &out_next_partition_id,
    int retry_count)
{
    std::string last_range_partition_table_name;
    last_range_partition_table_name.append(keyspace_name_);
    last_range_partition_table_name.append(".");
    last_range_partition_table_name.append(cass_last_range_id_name);

    // Select partition id
    std::string select_range_partition_str =
        "SELECT tablename, last_partition_id FROM ";
    select_range_partition_str.append(last_range_partition_table_name);
    select_range_partition_str.append(" WHERE tablename= ?");
    // Update update partition id
    std::string update_range_partition_str = "UPDATE ";
    update_range_partition_str.append(last_range_partition_table_name);
    update_range_partition_str.append(
        " SET last_partition_id= ? WHERE "
        "tablename = ? IF last_partition_id = ?");
    int32_t last_partition_id = -1;
    int32_t next_partition_id = -1;
    cass_bool_t update_result = cass_false;

    assert(table_name.Engine() != TableEngine::None);
    std::string table_key = table_name.Serialize();

    for (int i = 0; i < retry_count; i++)
    {
        bool select_result = ExecuteSelectStatement(
            session_,
            select_range_partition_str,
            [&table_key](CassStatement *stmt)
            { cass_statement_bind_string(stmt, 0, table_key.data()); },
            [&last_partition_id](const CassRow *row) -> bool
            {
                cass_value_get_int32(cass_row_get_column(row, 1),
                                     &last_partition_id);
                return false;
            },
            1);

        if (!select_result)
        {
            continue;
        }

        ExecuteSelectStatement(
            session_,
            update_range_partition_str,
            [&table_key, &last_partition_id, &range_cnt](CassStatement *stmt)
            {
                cass_statement_bind_int32(
                    stmt, 0, last_partition_id + range_cnt);
                cass_statement_bind_string(stmt, 1, table_key.data());
                cass_statement_bind_int32(stmt, 2, last_partition_id);
            },
            [&update_result,
             &last_partition_id,
             &next_partition_id,
             &range_cnt](const CassRow *row) -> bool
            {
                cass_value_get_bool(cass_row_get_column(row, 0),
                                    &update_result);
                if (update_result != cass_true)
                {
                    cass_value_get_int32(cass_row_get_column(row, 1),
                                         &last_partition_id);
                }
                else
                {
                    next_partition_id = last_partition_id + range_cnt;
                }
                return false;
            },
            1);

        if (update_result == cass_true)
        {
            break;
        }
    }

    if (update_result == cass_true)
    {
        out_next_partition_id = last_partition_id + 1;
        return true;
    }
    else
    {
        out_next_partition_id = -1;
        return false;
    }
}

EloqDS::CassBatchExecutor::CassBatchExecutor(CassSession *session)
    : session_(session), current_batch_size_(0), batch_tuple_size_(0)
{
    batch_ = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    cass_batch_set_is_idempotent(batch_, cass_true);
}

EloqDS::CassBatchExecutor::~CassBatchExecutor()
{
    session_ = nullptr;
    if (batch_ != nullptr)
    {
        cass_batch_free(batch_);
        batch_ = nullptr;
    }
    for (auto fut_it = futures_.begin(); fut_it != futures_.end(); fut_it++)
    {
        if (std::get<FUTURE>(*fut_it))
        {
            cass_future_free(std::get<FUTURE>(*fut_it));
        }

        if (std::get<BATCH>(*fut_it))
        {
            cass_batch_free(std::get<BATCH>(*fut_it));
        }
    }
}

/**
 * @brief Add a new statement to batch. If the batch is full, send the current
 * batch to Cassandra and reset the batch.
 *
 * @param stmt
 * @return CassError
 */
CassError EloqDS::CassBatchExecutor::AddBatchStatement(CassStatement *stmt,
                                                       uint32_t tuple_byte_size)
{
    if (batch_ == nullptr)
    {
        batch_ = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        cass_batch_set_is_idempotent(batch_, cass_true);
    }

    assert(batch_tuple_size_ < MaxTuplesBytesSize);
    CassError ce = cass_batch_add_statement(batch_, stmt);
    cass_statement_free(stmt);

    if (ce != CASS_OK)
    {
        LOG(ERROR) << "Add Batch Statement Error: " << ce;
        return ce;
    }

    batch_tuple_size_ += tuple_byte_size;
    current_batch_size_++;
    if (IsFull())
    {
        Execute();
    }

    return ce;
}

/**
 * @brief Send current batch request to Cassandra and add returned future to
 * futures_.
 *
 */
void EloqDS::CassBatchExecutor::Execute()
{
    if (!batch_)
    {
        assert(current_batch_size_ == 0);
        return;
    }
    futures_.emplace_back(cass_session_execute_batch(session_, batch_),
                          batch_,
                          current_batch_size_);

    batch_ = nullptr;
    current_batch_size_ = 0;
    batch_tuple_size_ = 0;
}

/**
 * @brief Wait for previous batch futures to return. Remove success futures
 * from futures_ vector and keep the failing ones.
 */
CassError EloqDS::CassBatchExecutor::Wait()
{
    for (auto fut_it = futures_.begin(); fut_it != futures_.end();)
    {
        CassFuture *future = std::get<FUTURE>(*fut_it);
        if (future == nullptr)  // skip invalid future, it should be retried
        {
            fut_it++;
            continue;
        }
        CassError rc = cass_future_error_code(future);
        if (rc == CASS_OK)  // remove successful futures
        {
            cass_batch_free(std::get<BATCH>(*fut_it));

            // collect metrics: cass flush rows
            if (metrics::enable_kv_metrics)
            {
                auto batch_size = std::get<BATCH_SIZE>(*fut_it);
                switch (flush_table_type_)
                {
                case FlushTableType::Base:
                    metrics::kv_meter->Collect(
                        metrics::NAME_KV_FLUSH_ROWS_TOTAL, batch_size, "base");
                    break;
                case FlushTableType::Archive:
                    metrics::kv_meter->Collect(
                        metrics::NAME_KV_FLUSH_ROWS_TOTAL,
                        batch_size,
                        "archive");
                    break;
                default:
                    break;
                }
            }

            fut_it = futures_.erase(fut_it);
        }
        else
        {
            // Print error sql for notice
            const char *error_message;
            size_t error_message_length;
            cass_future_error_message(
                future, &error_message, &error_message_length);
            LOG(ERROR) << "CassBatchExecute Error: " << error_message;
            std::get<FUTURE>(*fut_it) = nullptr;
            ++fut_it;
        }
        // delete future pointer
        cass_future_free(future);
    }

    if (futures_.size() == 0)
    {
        return CASS_OK;
    }
    else
    {
        return CASS_ERROR_LAST_ENTRY;
    }
}

/**
 * @brief Retry the failed batch requests in futures_ vector.
 *
 * @return CassError
 */
CassError EloqDS::CassBatchExecutor::Retry()
{
    for (auto fut_it = futures_.begin(); fut_it != futures_.end(); fut_it++)
    {
        if (std::get<FUTURE>(*fut_it) == nullptr)
        {
            std::get<FUTURE>(*fut_it) =
                cass_session_execute_batch(session_, std::get<BATCH>(*fut_it));
        }
    }

    return Wait();
}

bool EloqDS::CassBatchExecutor::HasStatements()
{
    return current_batch_size_ > 0;
}

EloqDS::BatchReadExecutor::BatchReadExecutor(
    CassSession *session,
    uint32_t max_futures_size,
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema,
    std::vector<txservice::FlushRecord> &results)
    : session_(session),
      max_futures_size_(max_futures_size),
      table_name_(table_name),
      table_schema_(table_schema),
      results_(results)
{
}

EloqDS::BatchReadExecutor::~BatchReadExecutor()
{
    session_ = nullptr;

    for (auto fut_it = futures_.begin(); fut_it != futures_.end(); fut_it++)
    {
        if (std::get<0>(*fut_it))
        {
            cass_future_free(std::get<0>(*fut_it));
        }
        if (std::get<1>(*fut_it))
        {
            cass_statement_free(std::get<1>(*fut_it));
        }
    }
}

// add statement and execute
bool EloqDS::BatchReadExecutor::AddStatement(CassStatement *stmt)
{
    assert(futures_.size() < max_futures_size_);
    futures_.emplace_back(cass_session_execute(session_, stmt), stmt, nullptr);
    return true;
}

// add statement and execute
bool EloqDS::BatchReadExecutor::AddStatement(CassStatement *stmt,
                                             const TxKey *key)
{
    assert(futures_.size() < max_futures_size_);
    futures_.emplace_back(cass_session_execute(session_, stmt), stmt, key);
    return true;
}

CassError EloqDS::BatchReadExecutor::Wait()
{
    for (auto fut_it = futures_.begin(); fut_it != futures_.end();)
    {
        CassFuture *&future = std::get<0>(*fut_it);
        if (future == nullptr)  // skip invalid future, it should be retried
        {
            fut_it++;
            continue;
        }
        CassError rc = cass_future_error_code(future);
        if (rc == CASS_OK)  // remove successful futures
        {
            /* This will also block until the query returns */
            const CassResult *result = cass_future_get_result(future);
            /* The future can be freed immediately after getting the result
             * object
             */
            cass_future_free(future);
            ParseReadResult(result, std::get<2>(*fut_it));
            cass_statement_free(std::get<1>(*fut_it));
            fut_it = futures_.erase(fut_it);
            cass_result_free(result);
        }
        else
        {
            // Print error sql for notice
            const char *error_message;
            size_t error_message_length;
            cass_future_error_message(
                future, &error_message, &error_message_length);
            LOG(ERROR) << "ParallelCassStmtExecute Error: " << error_message;
            // delete future pointer
            cass_future_free(future);
            future = nullptr;

            ++fut_it;
        }
    }

    if (futures_.size() == 0)
    {
        return CASS_OK;
    }
    else
    {
        return CASS_ERROR_LAST_ENTRY;
    }
}
CassError EloqDS::BatchReadExecutor::Retry()
{
    for (auto fut_it = futures_.begin(); fut_it != futures_.end(); fut_it++)
    {
        if (std::get<0>(*fut_it) == nullptr)
        {
            std::get<0>(*fut_it) =
                cass_session_execute(session_, std::get<1>(*fut_it));
        }
    }

    return Wait();
}

void EloqDS::BatchReadExecutor::ParseReadResult(const CassResult *result,
                                                const TxKey *key)
{
    const CassRow *row = cass_result_first_row(result);

    if (row != NULL)
    {
        auto &ref = results_.emplace_back();
        ref.SetKey(key->GetShallowCopy());

        uint16_t record_col_cnt = 1;

        int64_t *ts = reinterpret_cast<int64_t *>(&ref.commit_ts_);
        cass_value_get_int64(cass_row_get_column(row, record_col_cnt + 2), ts);

        cass_bool_t deleted = cass_false;
        cass_value_get_bool(cass_row_get_column(row, record_col_cnt + 3),
                            &deleted);

        if (deleted == cass_false)
        {
            std::unique_ptr<TxRecord> eloq_rec =
                TxRecordFactory::CreateTxRecord();
            table_schema_->RecordSchema()->EncodeToTxRecord(
                table_name_, row, *eloq_rec);
            if (table_name_.Engine() == TableEngine::EloqKv)
            {
                assert(false);
                ref.SetNonVersionedPayload(eloq_rec.get());
            }
            else
            {
                assert(table_name_.Engine() != TableEngine::None);
                ref.SetVersionedPayload(std::move(eloq_rec));
            }

            ref.payload_status_ = txservice::RecordStatus::Normal;
        }
        else
        {
            ref.payload_status_ = txservice::RecordStatus::Deleted;
        }
    }
}

bool EloqDS::CassHandler::PutArchivesAll(
    uint32_t node_group,
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    std::vector<txservice::FlushRecord> &batch)
{
    if (batch.size() == 0)
    {
        return true;
    }
    std::string mvcc_table_name;
    mvcc_table_name.append(keyspace_name_);
    mvcc_table_name.append(".");
    mvcc_table_name.append(cass_mvcc_archive_name);

    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);

    const CassPrepared *insert_prepared =
        GetCachedPreparedStmt(mvcc_table_name, 0U, CassPreparedType::Insert);
    if (insert_prepared == nullptr)
    {
        std::string insert_str("INSERT INTO ");
        insert_str.append(mvcc_table_name);
        insert_str.append(
            " (kv_table_name, mono_key, commit_ts, deleted, payload, "
            "unpack_info) "
            "VALUES (?,?,?,?,?,?) USING TTL 86400");

        CassFuture *future = cass_session_prepare(session_, insert_str.c_str());
        if (!cass_future_wait_timed(future, future_wait_timeout))
        {
            cass_future_free(future);
            return false;
        }

        CassError rc = cass_future_error_code(future);
        if (rc != CASS_OK)
        {
            cass_future_free(future);
            return false;
        }
        else
        {
            insert_prepared = cass_future_get_prepared(future);
            insert_prepared = CachePreparedStmt(
                mvcc_table_name, 0U, insert_prepared, CassPreparedType::Insert);
            cass_future_free(future);
        }
    }

    CassBatchExecutor cass_batch(session_);
    cass_batch.flush_table_type_ = CassBatchExecutor::FlushTableType::Archive;

    size_t flush_idx = 0;
    while (flush_idx < batch.size() &&
           Sharder::Instance().LeaderTerm(node_group) > 0)
    {
        for (; cass_batch.PendingFutureCount() < max_futures_ &&
               flush_idx < batch.size();
             ++flush_idx)
        {
            using namespace txservice;

            txservice::FlushRecord &ref = batch.at(flush_idx);
            uint32_t tuple_size = 0;

            CassStatement *statement = cass_prepared_bind(insert_prepared);
            cass_statement_bind_string_n(
                statement, 0, kv_table_name.data(), kv_table_name.size());

            // bind ccentry key
            cass_statement_bind_bytes(
                statement,
                1,
                reinterpret_cast<const uint8_t *>(ref.Key().Data()),
                ref.Key().Size());
            tuple_size += ref.Key().Size();
            // bind commit_ts
            cass_statement_bind_int64(
                statement, 2, static_cast<int64_t>(ref.commit_ts_));
            // bind deleted
            cass_bool_t deleted = cass_bool_t::cass_false;
            if (ref.payload_status_ == RecordStatus::Deleted)
            {
                deleted = cass_bool_t::cass_true;
            }
            cass_statement_bind_bool(statement, 3, deleted);
            // bind payload and unpack_info
            if (ref.Payload() != nullptr)
            {
                const TxRecord *payload = ref.Payload();

                if (table_name.Type() == txservice::TableType::Primary ||
                    table_name.Type() == txservice::TableType::UniqueSecondary)
                {
                    cass_statement_bind_bytes(statement,
                                              4,
                                              reinterpret_cast<const uint8_t *>(
                                                  payload->EncodedBlobData()),
                                              payload->EncodedBlobSize());
                }
                else
                {
                    assert(table_name.Type() ==
                           txservice::TableType::Secondary);
                    cass_statement_bind_null(statement, 4);
                }

                if (payload->UnpackInfoSize() > 0)
                {
                    cass_statement_bind_bytes(statement,
                                              5,
                                              reinterpret_cast<const uint8_t *>(
                                                  payload->UnpackInfoData()),
                                              payload->UnpackInfoSize());
                }
                else
                {
                    // unpack info is null
                    cass_statement_bind_null(statement, 5);
                }

                tuple_size +=
                    (payload->EncodedBlobSize() + payload->UnpackInfoSize());
            }

            CassError rc = cass_batch.AddBatchStatement(statement, tuple_size);
            if (rc != CASS_OK)
            {
                return false;
            }
        }

        if (cass_batch.PendingFutureCount() >= max_futures_ ||
            flush_idx == batch.size())
        {
            uint retry = 0;
            cass_batch.Execute();
            CassError ce = cass_batch.Wait();
            while (retry < 5 && ce != CASS_OK)
            {
                retry++;
                ce = cass_batch.Retry();
            }
            if (ce != CASS_OK)
            {
                return false;
            }
        }
    }

    return flush_idx == batch.size();
}

bool EloqDS::CassHandler::CopyBaseToArchive(
    std::vector<std::pair<txservice::TxKey, int32_t>> &batch,
    uint32_t node_group,
    const txservice::TableName &table_name,
    const txservice::TableSchema *table_schema)
{
    const CassPrepared *read_prepared =
        GetReadPrepared(table_name, table_schema);
    if (read_prepared == nullptr)
    {
        return false;
    }

    std::vector<FlushRecord> archive_vec;
    archive_vec.reserve(write_batch_ * max_futures_);
    BatchReadExecutor read_executor(
        session_, max_futures_, table_name, table_schema, archive_vec);

    size_t flush_idx = 0;
    while (flush_idx < batch.size() &&
           Sharder::Instance().LeaderTerm(node_group) > 0)
    {
        archive_vec.clear();

        // Read {max_futures_ * write_batch_} records in each round.
        for (size_t b = 0; b < write_batch_; b++)
        {
            for (; read_executor.PendingFutureCount() < max_futures_ &&
                   flush_idx < batch.size();
                 ++flush_idx)
            {
                const TxKey &tx_key = batch[flush_idx].first;

                CassStatement *statement = cass_prepared_bind(read_prepared);
                cass_statement_set_is_idempotent(statement, cass_true);

                int32_t pk1 = batch[flush_idx].second;
                int16_t pk2 = -1;
                // Bind kv table name
                cass_statement_bind_string(statement,
                                           0,
                                           table_schema->GetKVCatalogInfo()
                                               ->GetKvTableName(table_name)
                                               .data());
                // Bind pk1
                cass_statement_bind_int32(statement, 1, pk1);
                // Bind pk2
                cass_statement_bind_int16(statement, 2, pk2);
                // Binds the key field
                cass_statement_bind_bytes(
                    statement,
                    3,
                    reinterpret_cast<const uint8_t *>(tx_key.Data()),
                    tx_key.Size());

                read_executor.AddStatement(statement, &tx_key);
            }
            // Wait for future result for every max_futures_.
            if (read_executor.PendingFutureCount() > 0)
            {
                uint retry = 0;
                CassError ce = read_executor.Wait();
                while (retry < 5 && ce != CASS_OK)
                {
                    retry++;
                    ce = read_executor.Retry();
                }
                if (ce != CASS_OK)
                {
                    return false;
                }
            }
        }

        bool ret = PutArchivesAll(node_group,
                                  table_name,
                                  table_schema->GetKVCatalogInfo(),
                                  archive_vec);
        if (!ret)
        {
            return false;
        }
    }

    return true;
}

/**
 * @brief Generate an UUID v4 using cassandra driver.
 *
 * @return std::string
 */
std::string EloqDS::CassHandler::GenerateUUID()
{
    CassUuid uuid;
    char uuid_str[CASS_UUID_STRING_LENGTH];
    CassUuidGen *uuid_gen = cass_uuid_gen_new();
    cass_uuid_gen_random(uuid_gen, &uuid);
    cass_uuid_string(uuid, uuid_str);
    cass_uuid_gen_free(uuid_gen);
    std::string res = std::string(uuid_str, CASS_UUID_STRING_LENGTH - 1);
    std::replace(res.begin(), res.end(), '-', '_');

    return res;
}

/**
 * @brief Generate CassCatalogInfo that contains kv table name and kv index
 * names for a new table. Return the serialized string of the new catalog info.
 *
 * @param table_name
 * @param schema
 * @return std::string
 */
std::string EloqDS::CassHandler::CreateKVCatalogInfo(
    const txservice::TableSchema *schema) const
{
    CassCatalogInfo cass_info;
    cass_info.kv_index_names_.clear();
    cass_info.kv_table_name_ = std::string("t").append(GenerateUUID());

    std::vector<txservice::TableName> index_names = schema->IndexNames();
    for (auto idx_it = index_names.begin(); idx_it < index_names.end();
         ++idx_it)
    {
        if (idx_it->Type() == txservice::TableType::Secondary)
        {
            cass_info.kv_index_names_.emplace(
                *idx_it, std::string("i").append(GenerateUUID()));
        }
        else
        {
            assert((idx_it->Type() == txservice::TableType::UniqueSecondary));
            cass_info.kv_index_names_.emplace(
                *idx_it, std::string("u").append(GenerateUUID()));
        }
    }

    return cass_info.Serialize();
}

/**
 * @brief Deserialize the catalog info string and return an unique_ptr of
 * type CassCatalogInfo.
 *
 * @param kv_info_str
 * @param offset
 * @return txservice::KVCatalogInfo::uptr
 */
txservice::KVCatalogInfo::uptr EloqDS::CassHandler::DeserializeKVCatalogInfo(
    const std::string &kv_info_str, size_t &offset) const
{
    CassCatalogInfo::uptr cass_info = std::make_unique<CassCatalogInfo>();
    cass_info->Deserialize(kv_info_str.data(), offset);
    return cass_info;
}

/**
 * @brief Generate new CassCatalogInfo that contains current kv table name and
 * new kv index names for altered table.
 * Return the serialized string of the altered catalog info.
 *
 * Note: out of this function, index table name in AlterTableInfo object is
 * formatted as <table_name><INDEX_NAME_PREFIX><index_name>.
 *
 * @param table_name
 * @param current_table_schema
 * @param alter_table_info
 * @return std::string, alter_table_info
 */
std::string EloqDS::CassHandler::CreateNewKVCatalogInfo(
    const txservice::TableName &table_name,
    const txservice::TableSchema *current_table_schema,
    txservice::AlterTableInfo &alter_table_info)
{
    // Get current kv catalog info.
    const CassCatalogInfo *current_cass_catalog_info =
        static_cast<const CassCatalogInfo *>(
            current_table_schema->GetKVCatalogInfo());

    std::string new_kv_info, kv_table_name, new_kv_index_names;

    /* kv table name using current table name */
    kv_table_name = current_cass_catalog_info->kv_table_name_;
    uint32_t kv_val_len = kv_table_name.length();
    new_kv_info
        .append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
        .append(kv_table_name.data(), kv_val_len);

    /* kv index names using new schema index names */
    // 1. remove dropped index kv name
    bool dropped = false;
    for (auto kv_index_it = current_cass_catalog_info->kv_index_names_.cbegin();
         kv_index_it != current_cass_catalog_info->kv_index_names_.cend();
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
    for (auto add_index_it = alter_table_info.index_add_names_.cbegin();
         alter_table_info.index_add_count_ > 0 &&
         add_index_it != alter_table_info.index_add_names_.cend();
         add_index_it++)
    {
        // get index kv table name
        std::string add_index_kv_name;
        if (add_index_it->first.Type() == txservice::TableType::Secondary)
        {
            add_index_kv_name = std::string("i").append(GenerateUUID());
        }
        else
        {
            assert(add_index_it->first.Type() ==
                   txservice::TableType::UniqueSecondary);
            add_index_kv_name = std::string("u").append(GenerateUUID());
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

EloqDS::CassCatalogInfo::CassCatalogInfo(const std::string &kv_table_name,
                                         const std::string &kv_index_names)
{
    std::stringstream ss(kv_index_names);
    std::istream_iterator<std::string> begin(ss);
    std::istream_iterator<std::string> end;
    std::vector<std::string> tokens(begin, end);
    for (auto it = tokens.begin(); it != tokens.end(); ++it)
    {
        TableType table_type = TableName::Type(*it);
        assert(table_type == txservice::TableType::Secondary ||
               table_type == txservice::TableType::UniqueSecondary);

        const std::string &index_name_str = *it;
        const std::string &kv_index_name_str = *(++it);
        const std::string &table_engine_str = *(++it);
        assert(table_engine_str.size() == 1);

        txservice::TableEngine table_engine =
            static_cast<txservice::TableEngine>(table_engine_str[0]);
        txservice::TableName index_name(
            index_name_str, table_type, table_engine);
        kv_index_names_.emplace(index_name, kv_index_name_str);
    }
    kv_table_name_ = kv_table_name;
}

void EloqDS::CassCatalogInfo::Deserialize(const char *buf, size_t &offset)
{
    if (buf[0] == '\0')
    {
        return;
    }
    uint32_t *len_ptr = (uint32_t *) (buf + offset);
    uint32_t len_val = *len_ptr;
    offset += sizeof(uint32_t);

    kv_table_name_ = std::string(buf + offset, len_val);
    offset += len_val;

    len_ptr = (uint32_t *) (buf + offset);
    len_val = *len_ptr;
    offset += sizeof(uint32_t);
    if (len_val != 0)
    {
        std::string index_names(buf + offset, len_val);
        offset += len_val;
        std::stringstream ss(index_names);
        std::istream_iterator<std::string> begin(ss);
        std::istream_iterator<std::string> end;
        std::vector<std::string> tokens(begin, end);
        for (auto it = tokens.begin(); it != tokens.end(); ++it)
        {
            TableType table_type = TableName::Type(*it);
            assert(table_type == txservice::TableType::Secondary ||
                   table_type == txservice::TableType::UniqueSecondary);

            const std::string &index_name_str = *it;
            const std::string &kv_index_name_str = *(++it);
            const std::string &table_engine_str = *(++it);
            assert(table_engine_str.size() == 1);

            txservice::TableEngine table_engine =
                static_cast<txservice::TableEngine>(table_engine_str.at(0));
            txservice::TableName index_table_name(
                index_name_str, table_type, table_engine);
            kv_index_names_.emplace(index_table_name, kv_index_name_str);
        }
    }
    else
    {
        kv_index_names_.clear();
    }
}

std::string EloqDS::CassCatalogInfo::Serialize() const
{
    std::string str;
    size_t len_sizeof = sizeof(uint32_t);
    uint32_t len_val = (uint32_t) kv_table_name_.size();
    char *len_ptr = reinterpret_cast<char *>(&len_val);
    str.append(len_ptr, len_sizeof);
    str.append(kv_table_name_.data(), len_val);

    std::string index_names;
    if (kv_index_names_.size() != 0)
    {
        for (auto it = kv_index_names_.cbegin(); it != kv_index_names_.cend();
             ++it)
        {
            index_names.append(it->first.StringView())
                .append(" ")
                .append(it->second)
                .append(" ")
                .append(1, static_cast<char>(it->first.Engine()))
                .append(" ");
        }
        // index_names.substr(0, index_names.size() - 1);
        index_names.erase(index_names.size() - 1);
    }
    else
    {
        index_names.clear();
    }
    len_val = (uint32_t) index_names.size();
    str.append(len_ptr, len_sizeof);
    str.append(index_names.data(), len_val);

    return str;
}

EloqDS::CassHandler::UpsertTableData::UpsertTableData(
    CassHandler *cass_hd,
    NodeGroupId node_group_id,
    const txservice::TableName *table_name,
    const txservice::TableSchema *old_table_schema,
    const txservice::TableSchema *new_table_schema,
    txservice::OperationType op_type,
    CassSession *session,
    uint64_t write_time,
    bool is_bootstrap,
    bool ddl_skip_kv,
    bool high_compression_ratio,
    std::shared_ptr<void> defer_unpin,
    txservice::CcHandlerResult<txservice::Void> *hd_res,
    txservice::TxService *tx_service,
    const txservice::AlterTableInfo *alter_table_info)
    : CallbackData(session, hd_res),
      cass_hd_(cass_hd),
      node_group_id_(node_group_id),
      table_name_(table_name),
      upserting_table_name_{nullptr},
      old_table_schema_(old_table_schema),
      new_table_schema_(new_table_schema),
      op_type_(op_type),
      write_time_(write_time),
      is_bootstrap_(is_bootstrap),
      ddl_skip_kv_(ddl_skip_kv),
      high_compression_ratio_(high_compression_ratio),
      tx_service_(tx_service),
      alter_table_info_(alter_table_info),
      defer_unpin_(std::move(defer_unpin))

{
    schema_ = (op_type == OperationType::DropTable ||
               op_type == OperationType::TruncateTable)
                  ? old_table_schema
                  : new_table_schema;

    uint index_cnt = schema_->IndexesSize();

    if (index_cnt != 0)
    {
        const std::unordered_map<
            uint16_t,
            std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
            *indexes = schema_->GetIndexes();

        indexes_it_ = indexes->cbegin();
    }
    switch (op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::DropTable:
        // base table + index tables + main UpsertTable thread
        ref_count_ = index_cnt + 2;
        break;
    case OperationType::TruncateTable:
        // base table + index tables + main UpsertTable thread
        ref_count_ = index_cnt + 2;
        break;
    case OperationType::Update:
        // main UpsertTable thread
        ref_count_ = 1;
        break;
    case OperationType::AddIndex:
        assert(alter_table_info_->index_add_count_ ==
               alter_table_info_->index_add_names_.size());
        // index tables + main UpsertTable thread
        ref_count_ = alter_table_info_->index_add_count_ + 1;
        break;
    case OperationType::DropIndex:
        assert(alter_table_info_->index_drop_count_ ==
               alter_table_info_->index_drop_names_.size());
        ref_count_ = alter_table_info_->index_drop_count_ + 1;
        break;
    default:
        LOG(ERROR)
            << "Unsupported command for UpsertTableData::UpsertTableData";
        break;
    }
}

bool EloqDS::CassHandler::UpsertTableData::HasSKTable()
{
    return schema_->IndexesSize() > 0;
}

bool EloqDS::CassHandler::UpsertTableData::IsSKTableIteratorEnd()
{
    switch (op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::DropTable:
    {
        uint index_cnt = schema_->IndexesSize();

        if (index_cnt != 0)
        {
            const std::unordered_map<
                uint16_t,
                std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
                *indexes = schema_->GetIndexes();

            return indexes_it_ == indexes->cend();
        }
        break;
    }
    case OperationType::AddIndex:
    {
        assert(alter_table_info_->index_add_count_ > 0);
        return add_indexes_it_ == alter_table_info_->index_add_names_.cend();
    }
    case OperationType::DropIndex:
    {
        assert(alter_table_info_->index_drop_count_ > 0);
        return drop_indexes_it_ == alter_table_info_->index_drop_names_.cend();
    }
    default:
        LOG(ERROR) << "Unsupported command for "
                      "UpsertTableData::SKTableIterEnd, op type: "
                   << static_cast<int32_t>(op_type_);
        break;
    }

    return true;
}

void EloqDS::CassHandler::UpsertTableData::
    RewindSKTableIteratorMarkFirstForUpserting()
{
    switch (op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::DropTable:
    {
        uint index_cnt = schema_->IndexesSize();

        if (index_cnt != 0)
        {
            const std::unordered_map<
                uint16_t,
                std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
                *indexes = schema_->GetIndexes();

            indexes_it_ = indexes->cbegin();

            const std::pair<txservice::TableName, txservice::SecondaryKeySchema>
                &key_pair = indexes_it_->second;
            upserting_table_name_ = &key_pair.first;
        }
        break;
    }
    case OperationType::AddIndex:
    {
        assert(alter_table_info_->index_add_count_ > 0 &&
               alter_table_info_->index_add_count_ ==
                   alter_table_info_->index_add_names_.size());
        add_indexes_it_ = alter_table_info_->index_add_names_.cbegin();
        upserting_table_name_ = &(add_indexes_it_->first);
        break;
    }
    case OperationType::DropIndex:
    {
        assert(alter_table_info_->index_drop_count_ > 0 &&
               alter_table_info_->index_drop_count_ ==
                   alter_table_info_->index_drop_names_.size());
        drop_indexes_it_ = alter_table_info_->index_drop_names_.cbegin();
        upserting_table_name_ = &(drop_indexes_it_->first);
        break;
    }
    default:
        LOG(ERROR) << "Unsupported command for UpsertTableData::RewindSKTable, "
                      "op type: "
                   << static_cast<int32_t>(op_type_);
        break;
    }
}

EloqDS::CassHandler::UpsertTableData *
EloqDS::CassHandler::UpsertTableData::MarkNextSKTableForUpserting()
{
    switch (op_type_)
    {
    case OperationType::CreateTable:
    case OperationType::DropTable:
    {
        assert(schema_->IndexesSize() > 0);

        const std::unordered_map<
            uint16_t,
            std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
            *indexes = schema_->GetIndexes();

        if (indexes_it_ == indexes->cend())
        {
            return this;
        }

        indexes_it_++;
        if (indexes_it_ == indexes->cend())
        {
            return this;
        }

        const std::pair<txservice::TableName, txservice::SecondaryKeySchema>
            &key_pair = indexes_it_->second;
        upserting_table_name_ = &key_pair.first;
        break;
    }
    case OperationType::AddIndex:
    {
        if (add_indexes_it_ != alter_table_info_->index_add_names_.cend() &&
            ++add_indexes_it_ != alter_table_info_->index_add_names_.cend())
        {
            upserting_table_name_ = &(add_indexes_it_->first);
        }
        break;
    }
    case OperationType::DropIndex:
    {
        if (drop_indexes_it_ != alter_table_info_->index_drop_names_.cend() &&
            ++drop_indexes_it_ != alter_table_info_->index_drop_names_.cend())
        {
            upserting_table_name_ = &(drop_indexes_it_->first);
        }
        break;
    }
    default:
        LOG(ERROR)
            << "Unsupported command for UpsertTableData::MarkNextSKTable, "
               "op type: "
            << static_cast<int32_t>(op_type_);
        break;
    }

    return this;
}

bool EloqDS::CassHandler::InitPreBuiltTables()
{
    for (const auto &[table_name, kv_table_name] : pre_built_table_names_)
    {
#ifdef RANGE_PARTITION_ENABLED
        assert(false && "mysql needs schema image");

        /*
        int32_t initial_partition_id= 0;
        if (table_name == Sequences::mysql_seq_string)
        {
          initial_partition_id= 0;
        }
        else
        {
          initial_partition_id= Partition::InitialPartitionId(table_name);
        }

        {
          assert(table_name.Engine() != TableEngine::None);
          std::string table_key = table_name.Serialize();

          std::string upsert_query("INSERT INTO ");
          upsert_query.append(keyspace_name_);
          upsert_query.append(".");
          upsert_query.append(cass_range_table_name);
          upsert_query.append(
              " (\"tablename\", \"___mono_key___\", \"___segment_id___\", "
              "\"___segment_cnt___\", \"___partition_id___\", "
              "\"___version___\", \"___slice_version___\") VALUES "
              "(?,?,?,?,?,?,?)");

          CassStatement *upsert_stmt= cass_statement_new(upsert_query.c_str(),
        7);
          // Bind tablename
          cass_statement_bind_string(upsert_stmt, 0, table_key.data());
          const TxKey *packed_neg_inf_key=
        TxKeyFactory::PackedNegativeInfinity();
          // Bind mono_key
          cass_statement_bind_bytes(
              upsert_stmt, 1,
              reinterpret_cast<const uint8_t *>(packed_neg_inf_key->Data()),
              packed_neg_inf_key->Size()); // mono_key

          // Bind segment id
          cass_statement_bind_int64(upsert_stmt, 2, 0);
          // Bind segment count
          cass_statement_bind_int64(upsert_stmt, 3, 1);
          // Bind partition_id
          cass_statement_bind_int32(upsert_stmt, 4,
                                    initial_partition_id); // partition_id
          // Bind version
          cass_statement_bind_int64(upsert_stmt, 5,
                                    1); // version
          // Bind slice_version
          cass_statement_bind_int64(upsert_stmt, 6, 1);

          CassFuture *upsert_future= cass_session_execute(session_,
        upsert_stmt); cass_future_wait(upsert_future); auto rc=
        cass_future_error_code(upsert_future); if (rc != CASS_OK)
          {
            LOG(ERROR) << "Insert initial partition id for " << table_name
                       << " failed. " << ErrorMessage(upsert_future);

            cass_statement_free(upsert_stmt);
            cass_future_free(upsert_future);
            return false;
          }
          else
          {
            LOG(ERROR) << "Insert initial partition id for " << table_name
                       << " succeed. ";

            cass_statement_free(upsert_stmt);
            cass_future_free(upsert_future);
          }
        }

        {
          std::string insert_range_partition_id_str= "INSERT INTO ";
          insert_range_partition_id_str.append(keyspace_name_);
          insert_range_partition_id_str.append(".");
          insert_range_partition_id_str.append(cass_last_range_id_name);
          insert_range_partition_id_str.append(" (tablename, "
                                               "last_partition_id) VALUES (?,
        ?)");

          CassStatement *upsert_stmt=
              cass_statement_new(insert_range_partition_id_str.c_str(), 2);
          cass_statement_bind_string(upsert_stmt, 0, table_key.data());
          cass_statement_bind_int32(upsert_stmt, 1, initial_partition_id);
          CassFuture *future= cass_session_execute(session_, upsert_stmt);
          cass_future_wait(future);
          auto rc= cass_future_error_code(future);
          cass_statement_free(upsert_stmt);
          cass_future_free(future);
          if (rc != CASS_OK)
          {
            LOG(ERROR) << "Insert last partition id for " << table_name
                       << " failed. " << ErrorMessage(future);
            return false;
          }
          else
          {
            LOG(ERROR) << "Insert last partition id for " << table_name
                       << " succeed. ";
          }
        }
        */

#endif
        assert(table_name.Engine() == txservice::TableEngine::EloqKv);
        std::string table_key = table_name.Serialize();

        // insert table into catalog table
        // create table catalog information stored in eloqkv_tables
        // Inserts an entire row or upserts data into an existing row,
        // using the full primary key.
        std::string insert_str("INSERT INTO ");
        insert_str.append(keyspace_name_);
        insert_str.append(".");
        insert_str.append(cass_table_catalog_name);
        insert_str.append(
            " (tablename, content, kvtablename, kvindexname, keyschemasts, "
            "version)"
            " VALUES ('");
        // tablename
        insert_str.append(table_key);
        // content
        insert_str.append("', 0x");
        insert_str.append(", '");
        // kvtablename
        insert_str.append(kv_table_name);
        // kvindexname
        insert_str.append("', ''");
        insert_str.append(", '");
        // keyschemats, use table version as key schema ts
        uint64_t table_version = 100U;
        std::string table_verions_str = std::to_string(table_version);
        const std::string &key_schema_ts_str = table_verions_str;
        insert_str.append(key_schema_ts_str);
        insert_str.append("', ");
        // tableversion
        insert_str.append(table_verions_str);

        insert_str.append(") USING TIMESTAMP ");
        // the timestamp is treated as commit ts of the DDL txn which cannot be
        // 0, 1 indicates non-existent, so just use 2
        insert_str.append(table_verions_str);

        CassStatement *insert_catalog_stmt =
            cass_statement_new(insert_str.c_str(), 0);
        CassFuture *insert_catalog_future =
            cass_session_execute(session_, insert_catalog_stmt);

        cass_future_wait(insert_catalog_future);
        CassError rc = cass_future_error_code(insert_catalog_future);

        if (rc == CASS_OK)
        {
            LOG(INFO) << "Created table: " << table_name.StringView()
                      << std::endl;

            cass_future_free(insert_catalog_future);
            cass_statement_free(insert_catalog_stmt);
        }
        else
        {
            LOG(INFO) << "Failed to create table: " << table_name.StringView()
                      << ", error:" << ErrorMessage(insert_catalog_future)
                      << std::endl;

            cass_future_free(insert_catalog_future);
            cass_statement_free(insert_catalog_stmt);
            return false;
        }
    }

    return true;
}
