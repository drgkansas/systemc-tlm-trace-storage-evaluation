/******************************************************************************
 *                                                                            *
 * Simplified placeholder for a Zarr-backed database.                         *
 * This implementation is a stub: it overrides all required hooks but         *
 * does not persist data. It only prints when fw/bw transactions arrive.      *
 *                                                                            *
 ******************************************************************************/

#ifndef INSCIGHT_DATABASE_ZARR_H
#define INSCIGHT_DATABASE_ZARR_H

#include <string>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>

#include "inscight/entry.h"
#include "inscight/database.h"

// z5 includes for Zarr functionality
#include "z5/factory.hxx"
#include "z5/filesystem/handle.hxx"
#include "z5/multiarray/xtensor_access.hxx"
#include "z5/attributes.hxx"

namespace inscight
{

    class database_zarr : public database
    {
    private:
        // Zarr datasets for storing transaction data (matching SQLite schema)
        std::unique_ptr<z5::Dataset> idDs;         // id field
        std::unique_ptr<z5::Dataset> stDs;         // st (timestamp) field
        std::unique_ptr<z5::Dataset> dirDs;        // dir (direction: 0=FW, 1=BW) field
        std::unique_ptr<z5::Dataset> portDs;       // port field
        std::unique_ptr<z5::Dataset> protoDs;      // proto (protocol) field
        std::unique_ptr<z5::Dataset> jsonDs;       // json (full JSON text) field
        size_t entryCount;

	size_t maxEntries;   // total rows per store
        size_t jsonWidth;    // bytes per JSON record
			     //
        // Helper methods
        void storeTransactionData(id_t obj, sysc_time_t timestamp, protocol_kind proto, const char* json, int direction);
	void flushChunk();

	static constexpr std::size_t chunkSize = 1000;   // match your Zarr chunks {1000} / {1000, jsonWidth}

    // in-memory buffers for one chunk
    xt::xarray<uint64_t> idBuf;
    xt::xarray<uint64_t> stBuf;
    xt::xarray<int32_t>  dirBuf;
    xt::xarray<uint64_t> portBuf;
    xt::xarray<int32_t>  protoBuf;
    xt::xarray<uint8_t>  jsonBuf;   // shape: (chunkSize, jsonWidth)

    std::size_t bufCount    = 0;    // how many entries currently buffered
    std::size_t flushBaseIdx = 0;   // starting index in the dataset for the current buffer


    protected:
        virtual void init() override;
        virtual void begin(size_t) override {}
        virtual void end(size_t) override {}

        virtual void gen_meta(const meta_info &info) override;

        // Object lifecycle
        virtual void module_created(id_t, const char *, const char *) override {}
        virtual void process_created(id_t, const char *, proc_kind) override {}
        virtual void port_created(id_t, const char *) override {}
        virtual void event_created(id_t, const char *) override {}
        virtual void channel_created(id_t, const char *, const char *) override {}

        // Topology
        virtual void port_bound(id_t, id_t, binding_kind, protocol_kind) override {}

        // Elaboration phases
        virtual void module_phase_started(id_t, module_phase, real_time_t) override {}
        virtual void module_phase_finished(id_t, module_phase, real_time_t) override {}

        // Scheduling
        virtual void process_start(id_t, real_time_t, sysc_time_t) override {}
        virtual void process_yield(id_t, real_time_t, sysc_time_t) override {}

        // Events
        virtual void event_notify_immediate(id_t, real_time_t, sysc_time_t) override {}
        virtual void event_notify_delta(id_t, real_time_t, sysc_time_t) override {}
        virtual void event_notify_timed(id_t, real_time_t, sysc_time_t, sysc_time_t) override {}
        virtual void event_cancel(id_t, real_time_t, sysc_time_t) override {}

        // Channel updates
        virtual void channel_update_start(id_t, real_time_t, sysc_time_t) override {}
        virtual void channel_update_complete(id_t, real_time_t, sysc_time_t) override {}

        // CPU
        virtual void cpu_idle_enter(id_t, sysc_time_t) override {}
        virtual void cpu_idle_leave(id_t, sysc_time_t) override {}
        virtual void cpu_call_stack(id_t, sysc_time_t, size_t, unsigned long long, const char *) override {}

        // Transactions
        virtual void transaction_trace_fw(id_t obj, sysc_time_t st, protocol_kind proto, const char *json) override;
        virtual void transaction_trace_bw(id_t obj, sysc_time_t st, protocol_kind proto, const char *json) override;

        // Logs/quantum
        virtual void log_message(sysc_time_t, int, const char *, const char *) override {}
        virtual void quantum_update(sysc_time_t, sysc_time_t, sysc_time_t) override {}

        // Kernel/kthread/irq
        virtual void handle_kthread_event(real_time_t, kthread_event) override {}
        virtual void handle_irq_event(id_t, real_time_t, sysc_time_t, size_t, irq_event) override {}

    public:
        //database_zarr(const std::string &options) : database(options), entryCount(0) {}


	        // NEW: initialize capacity and JSON width with reasonable defaults
        database_zarr(const std::string &options)
            : database(options),
              entryCount(0),
              maxEntries(200000000),  // change this if you want a different cap
              jsonWidth(512)           // matches your current layout
        {}



	virtual ~database_zarr();
        
        // Public method to retrieve stored transaction data
        void retrieveTransactionData(size_t index);
    };

} // namespace inscight

#endif

