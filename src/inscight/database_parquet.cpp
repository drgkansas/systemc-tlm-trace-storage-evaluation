/******************************************************************************
 *                                                                            *
 * Copyright 2023 MachineWare GmbH                                            *
 * All Rights Reserved                                                        *
 *                                                                            *
 * This is unpublished proprietary work owned by MachineWare GmbH. It may be  *
 * used, modified and distributed in accordance to the license specified by   *
 * the license file in the root directory of this project.         
 * 
 * Changes for parquet database 
 * Copyright 2025
 * Modifications by George Frazier           *
 *                                                                            *
 ******************************************************************************/

#include "inscight/database_parquet.h"
#include <cstdint>
#include <sstream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unistd.h>
#include <cstdlib>
#include <atomic>
#include <vector>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/api/writer.h>

#ifdef _MSC_VER
#include <Windows.h>
#else
#include <unistd.h>
#endif

#define SQL_ERROR(...) do {       \
    fprintf(stderr, __VA_ARGS__); \
    fprintf(stderr, "\n");        \
    fflush(stderr);               \
    abort();                      \
} while (0)

#define SQL_ERROR_ON(ret, ...) do { \
    if (ret != SQLITE_OK) {         \
        SQL_ERROR(__VA_ARGS__);     \
    }                               \
} while (0)

namespace inscight {

std::atomic<std::uint64_t> unique_transaction_id{1};
std::atomic<std::uint64_t> unique_fw_transaction_id{1};
std::atomic<std::uint64_t> unique_bw_transaction_id{1};
std::chrono::steady_clock::time_point start_time;

bool firstTransactionWritten = false;

void database_parquet::init() {
    parquet_init_writer(); 
}

void database_parquet::begin(size_t n) {
}

void database_parquet::end(size_t n) {
}

void database_parquet::transaction_trace_fw(id_t obj, sysc_time_t st, protocol_kind proto, const char* json) {
    unsigned long long txn_id = unique_transaction_id.fetch_add(1, std::memory_order_relaxed);
    unique_fw_transaction_id.fetch_add(1, std::memory_order_relaxed);

    // Parquet buffer
    {
        std::lock_guard<std::mutex> lock(m_parquet_mutex);


        std::int64_t time_ps = static_cast<std::int64_t>(st);
        std::int8_t  dir     = 0;
        std::int16_t p       = static_cast<std::int16_t>(proto);

        m_parquet_buffer.emplace_back(
            m_sim_id,
            static_cast<std::uint64_t>(obj),
            static_cast<std::uint64_t>(txn_id),
            time_ps,
            dir,
            p,
            std::string(json ? json : "")
        );

	// Instrument for SystemC paper metrics
	if (txn_id % DB_INSTRUMENT_SIZE == 0){
            auto now = std::chrono::steady_clock::now();
	    auto ms =  std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
	    std::cerr << "TX_CHECKOUT " << txn_id << " transactions " <<  ms << " ms" << std::endl << std::flush; 
	}

        if (m_parquet_buffer.size() >= PARQUET_BATCH_SIZE) {
            parquet_flush_locked();
        }
    }
}

void database_parquet::transaction_trace_bw(id_t obj, sysc_time_t st, protocol_kind proto, const char* json) {
    unsigned long long txn_id = unique_transaction_id.fetch_add(1, std::memory_order_relaxed);
    unique_bw_transaction_id.fetch_add(1, std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lock(m_parquet_mutex);

        std::int64_t time_ps = static_cast<std::int64_t>(st);
        std::int8_t  dir     = 1;
        std::int16_t p       = static_cast<std::int16_t>(proto);

        m_parquet_buffer.emplace_back(
            m_sim_id,
            static_cast<std::uint64_t>(obj),
            static_cast<std::uint64_t>(txn_id),
            time_ps,
            dir,
            p,
            std::string(json ? json : "")
        );

	// Instrument for SystemC paper metrics
        if (txn_id % DB_INSTRUMENT_SIZE == 0){
            auto now = std::chrono::steady_clock::now();
            auto ms =  std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
            std::cerr << "TX_CHECKOUT " << txn_id << " transactions " << ms << " ms" << std::endl   << std::flush;
        }

        if (m_parquet_buffer.size() >= PARQUET_BATCH_SIZE) {
            parquet_flush_locked();
        }
    }
}

database_parquet::database_parquet(const std::string& options):
    database(options)
{
   //std::cerr << "database_parquet constructor\n";
}


database_parquet::~database_parquet() {
    std::cerr << "In parquet destructor" << std::endl << std::flush;

    if (!firstTransactionWritten){
       parquet_flush_locked();
    }

    unsigned long long txn_id = unique_transaction_id.fetch_add(1, std::memory_order_relaxed);
    unique_fw_transaction_id.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "TX_CHECKOUT Number of fw transactions " << unique_fw_transaction_id.fetch_add(1, std::memory_order_relaxed) - 1 << std::endl  << std::flush;
    std::cerr << "TX_CHECKOUT Number of bw transactions " << unique_bw_transaction_id.fetch_add(1, std::memory_order_relaxed) - 1 << std::endl  << std::flush;
    std::cerr << "TX_CHECKOUT Number of transactions " << unique_transaction_id.fetch_add(1, std::memory_order_relaxed) - 1 << std::endl  << std::flush;
    stop();
}


#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>


void database_parquet::parquet_init_writer() {
    // 1) Define schema
    m_parquet_schema = arrow::schema({
        arrow::field("sim_id",    arrow::uint64()),
        arrow::field("port_id",   arrow::uint64()),
        arrow::field("txn_id",    arrow::uint64()),
        arrow::field("time_ps",   arrow::int64()),
        arrow::field("direction", arrow::int8()),
        arrow::field("protocol",  arrow::int16()),
        arrow::field("payload",   arrow::utf8())
    });

    // 2) Build filename prefix (one prefix per process)
    char fname[256];
    std::snprintf(fname, sizeof(fname), "transactions_%d", (int)getpid());
    m_parquet_prefix = fname;

    m_parquet_chunk_index = 0; // start chunks at 0
}


void database_parquet::parquet_append_rows_locked(const std::vector<parquet_row_t>& rows) {
    if (rows.empty()) {
        return;
    }

    arrow::UInt64Builder sim_builder, port_builder, txn_builder;
    arrow::Int64Builder  time_builder;
    arrow::Int8Builder   dir_builder;
    arrow::Int16Builder  proto_builder;
    arrow::StringBuilder payload_builder;

    for (const auto& row : rows) {
        std::uint64_t sim_id;
        std::uint64_t port_id;
        std::uint64_t txn_id;
        std::int64_t  time_ps;
        std::int8_t   dir;
        std::int16_t  proto;
        const std::string& payload = std::get<6>(row);

        std::tie(sim_id, port_id, txn_id, time_ps, dir, proto, std::ignore) = row;

        PARQUET_THROW_NOT_OK(sim_builder.Append(sim_id));
        PARQUET_THROW_NOT_OK(port_builder.Append(port_id));
        PARQUET_THROW_NOT_OK(txn_builder.Append(txn_id));
        PARQUET_THROW_NOT_OK(time_builder.Append(time_ps));
        PARQUET_THROW_NOT_OK(dir_builder.Append(dir));
        PARQUET_THROW_NOT_OK(proto_builder.Append(proto));
        PARQUET_THROW_NOT_OK(payload_builder.Append(payload));
    }

    std::shared_ptr<arrow::Array> sim_arr, port_arr, txn_arr;
    std::shared_ptr<arrow::Array> time_arr, dir_arr, proto_arr, payload_arr;

    PARQUET_THROW_NOT_OK(sim_builder.Finish(&sim_arr));
    PARQUET_THROW_NOT_OK(port_builder.Finish(&port_arr));
    PARQUET_THROW_NOT_OK(txn_builder.Finish(&txn_arr));
    PARQUET_THROW_NOT_OK(time_builder.Finish(&time_arr));
    PARQUET_THROW_NOT_OK(dir_builder.Finish(&dir_arr));
    PARQUET_THROW_NOT_OK(proto_builder.Finish(&proto_arr));
    PARQUET_THROW_NOT_OK(payload_builder.Finish(&payload_arr));

    auto table = arrow::Table::Make(
        m_parquet_schema,
        { sim_arr, port_arr, txn_arr, time_arr, dir_arr, proto_arr, payload_arr }
    );

    // Build a unique chunk file name: transactions_<pid>_chunk_<n>.parquet
    char fname[512];
    std::snprintf(fname, sizeof(fname), "%s_chunk_%zu.parquet",
                  m_parquet_prefix.c_str(), m_parquet_chunk_index++);
    std::string file_name(fname);

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(file_name)
    );

    // This writes a complete, valid Parquet file (with footer)
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(
            *table,
            arrow::default_memory_pool(),
            outfile,
            /*chunk_size=*/64 * 1024
        )
    );

   // std::cerr << "Wrote Parquet chunk: " << file_name
              //<< " rows=" << rows.size() << "\n";
}

void database_parquet::gen_meta(const meta_info& info) {
    // Use pid as a simple sim_id for the Parquet logs
    m_sim_id = static_cast<std::uint64_t>(info.pid);
}


void database_parquet::parquet_flush_locked() {
    if (m_parquet_buffer.empty()) {
        return;
    }
    firstTransactionWritten = true;
    parquet_append_rows_locked(m_parquet_buffer);
    m_parquet_buffer.clear();
}


} // namespace inscight
