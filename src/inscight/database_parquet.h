/******************************************************************************
 *                                                                            *
 * Copyright 2023 MachineWare GmbH                                            *
 * All Rights Reserved                                                        *
 *                                                                            *
 * This is unpublished proprietary work owned by MachineWare GmbH. It may be  *
 * used, modified and distributed in accordance to the license specified by   *
 * the license file in the root directory of this project.                    *
 * 
 * 
 * Changes for parquet database 
 * Copyright 2025
 * Modifications by George Frazier  
 *                                                                            *
 ******************************************************************************/

#ifndef INSCIGHT_DATABASE_PARQUET_H
#define INSCIGHT_DATABASE_PARQUET_H

#include <string>

#include "inscight/entry.h"
#include "inscight/database.h"

#include <memory>
#include <mutex>
#include <vector>
#include <tuple>

// forward decls to avoid dragging Arrow headers into the header
namespace arrow {
    class Schema;
}

namespace inscight {

class database_parquet : public database
{
private:
    // Parquet-related
    using parquet_row_t =
        std::tuple<std::uint64_t, std::uint64_t, std::uint64_t,
                   std::int64_t, std::int8_t, std::int16_t, std::string>;
                   // sim_id, port_id, txn_id, time_ps, dir, proto, payload

    std::uint64_t m_sim_id = 0;  // from meta_info.pid

    std::mutex m_parquet_mutex;
    std::vector<parquet_row_t> m_parquet_buffer;

    std::shared_ptr<arrow::Schema> m_parquet_schema;

    //static constexpr std::size_t PARQUET_BATCH_SIZE = 50000;
    static constexpr std::size_t PARQUET_BATCH_SIZE = 100000;

    // NEW: prefix for chunk file names and a chunk counter
    std::string m_parquet_prefix;          // e.g. "transactions_<pid>"
    std::size_t m_parquet_chunk_index = 0; // increments per written chunk


    void parquet_init_writer();
    void parquet_append_rows_locked(const std::vector<parquet_row_t>& rows);
    void parquet_flush_locked();




protected:
    virtual void init() override;
    virtual void begin(size_t n) override;
    virtual void end(size_t n) override;

    virtual void gen_meta(const meta_info& info) override;

    virtual void module_created(id_t obj, const char* name, const char* kind) override {}
    virtual void process_created(id_t obj, const char* name, proc_kind kind) override {}
    virtual void port_created(id_t obj, const char* name) override {}
    virtual void event_created(id_t obj, const char* name) override {}
    virtual void channel_created(id_t obj, const char* name, const char* kind) override{}

    virtual void port_bound(id_t from, id_t to, binding_kind kind, protocol_kind proto) override {}


    virtual void quantum_update(sysc_time_t st, sysc_time_t oldq, sysc_time_t newq){}
    virtual void handle_kthread_event(real_time_t rt, kthread_event event) {}
    virtual void handle_irq_event(id_t obj, real_time_t rt, sysc_time_t st, size_t irqid, irq_event event) {}

    virtual void module_phase_started(id_t obj, module_phase phase, real_time_t t) override {}
    virtual void module_phase_finished(id_t obj, module_phase phase, real_time_t t) override {}

    virtual void process_start(id_t obj, real_time_t rt, sysc_time_t st) override {}
    virtual void process_yield(id_t obj, real_time_t rt, sysc_time_t st) override {}

    virtual void event_notify_immediate(id_t obj, real_time_t rt, sysc_time_t st) override {}
    virtual void event_notify_delta(id_t obj, real_time_t rt, sysc_time_t st) override {}
    virtual void event_notify_timed(id_t obj, real_time_t rt, sysc_time_t st, sysc_time_t delay) override {}
    virtual void event_cancel(id_t obj, real_time_t rt, sysc_time_t st) override {}

    virtual void channel_update_start(id_t obj, real_time_t rt, sysc_time_t st) override {}
    virtual void channel_update_complete(id_t obj, real_time_t rt, sysc_time_t st) override {}

    virtual void cpu_idle_enter(id_t obj, sysc_time_t st) override {}
    virtual void cpu_idle_leave(id_t obj, sysc_time_t st) override {}

    virtual void cpu_call_stack(id_t obj, sysc_time_t st, size_t level, unsigned long long addr, const char* sym) override {}

    virtual void transaction_trace_fw(id_t obj, sysc_time_t st, protocol_kind proto, const char* json) override;
    virtual void transaction_trace_bw(id_t obj, sysc_time_t st, protocol_kind proto, const char* json) override;

    virtual void log_message(sysc_time_t st, int loglevel, const char* sender, const char* message) override {}

public:
    database_parquet(const std::string& options);
    virtual ~database_parquet();
};

} // namespace inscight

#endif
