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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "data_store_service_client.h"
#include "eloq_key.h"
#include "schema.h"
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_record.h"

namespace EloqDS
{
#if ROCKSDB_CLOUD_FS()
class DataStoreServiceScanner : public txservice::store::DataStoreScanner
{
public:
    DataStoreServiceScanner(
        DataStoreServiceClient *client,
        const txservice::KeySchema *key_sch,
        const txservice::RecordSchema *rec_sch,
        const txservice::TableName &table_name,
        const txservice::KVCatalogInfo *kv_info,
        const txservice::TxKey &start_key,
        bool inclusive,
        const std::vector<txservice::store::DataStoreSearchCond> &pushdown_cond,
        bool scan_forward);

    ~DataStoreServiceScanner();

    bool Init();
    bool MoveNext() override;
    void Current(txservice::TxKey &key,
                 const txservice::TxRecord *&rec,
                 uint64_t &version_ts,
                 bool &deleted) override;
    void End() override;

private:
    bool FetchNextBatch();
    bool IsRunOutOfData();
    bool CloseScan();

private:
    // data store client
    DataStoreServiceClient *client_;

    // scanner parameters
    const txservice::KeySchema *key_sch_;
    const txservice::RecordSchema *rec_sch_;
    const txservice::TableName table_name_;
    const txservice::KVCatalogInfo *kv_info_;
    const bool inclusive_;
    bool scan_forward_;
    const std::vector<txservice::store::DataStoreSearchCond>
        pushdown_condition_;
    const size_t batch_size_;

    // scanner state
    bool initialized_;
    // the last key of the previous batch
    // when initialized is false, last_key_ is the start key of the scan
    // when initialized is true, result_cache_ is empty and last_key_ is null,
    // the scanner is run out of data
    std::unique_ptr<std::string> last_key_;
    bool last_key_is_neg_inf_{false};
    bool last_key_is_pos_inf_{false};
    // the session id of the current scan map to the state of the scan on the
    // server
    std::string session_id_;
    EloqDS::remote::ScanRequest request_;

    // result cache
    EloqDS::remote::ScanResponse::Item current_item_;
    std::unique_ptr<EloqKV::EloqKey> current_key_{nullptr};
    txservice::TxRecord::Uptr current_rec_{nullptr};
    std::deque<EloqDS::remote::ScanResponse::Item> result_cache_;
};
#endif
}  // namespace EloqDS
