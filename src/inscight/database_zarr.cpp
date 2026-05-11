/******************************************************************************
 *                                                                            *
 * Minimal no-op implementation for Zarr backend tracing.                     *
 * Prints in fw/bw transaction hooks to verify call paths.                    *
 *                                                                            *
 ******************************************************************************/

#include <nlohmann/json.hpp>
#include <xtensor/containers/xarray.hpp>

// factory functions to create files, groups and datasets
#include <z5/factory.hxx>
// handles for z5 filesystem objects
#include <z5/filesystem/handle.hxx>
// io for xtensor multi-arrays
#include <z5/multiarray/xtensor_access.hxx>
// attribute functionality
#include <z5/attributes.hxx>

#include "inscight/database_zarr.h"
#include <array>
#include <iostream>
#include <fstream>

namespace inscight
{

extern std::atomic<std::uint64_t> unique_transaction_id;
extern std::atomic<std::uint64_t> unique_fw_transaction_id;
extern std::atomic<std::uint64_t> unique_bw_transaction_id;
extern std::chrono::steady_clock::time_point start_time;

 


	// Helper function to fix zarr metadata fill_value from float to integer
    void fixZarrMetadata(const std::string& metadataPath) {
        std::ifstream inFile(metadataPath);
        if (!inFile.is_open()) {
            std::cerr << "[database_zarr] Warning: Could not open " << metadataPath << " for reading" << std::endl;
            return;
        }

        nlohmann::json metadata;
        try {
            inFile >> metadata;
            inFile.close();

            // Fix fill_value from 0.0 to 0 for integer types
            if (metadata.contains("fill_value") && metadata["fill_value"] == 0.0) {
                metadata["fill_value"] = 0;

                std::ofstream outFile(metadataPath);
                if (outFile.is_open()) {
                    outFile << metadata.dump(4) << std::endl;
                    outFile.close();
                    //std::cout << "[database_zarr] Fixed fill_value in " << metadataPath << std::endl;
                } else {
                    std::cerr << "[database_zarr] Warning: Could not write to " << metadataPath << std::endl;
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[database_zarr] Error fixing metadata " << metadataPath << ": " << e.what() << std::endl;
        }
    }

    void database_zarr::init()
    {
        std::cout << "[database_zarr] init()" << std::endl;

        // get handle to a File on the filesystem
        z5::filesystem::handle::File f("data.zr");
        //std::cout << "[database_zarr] get handle to a File on the filesystem" << std::endl;

        // create the file in zarr format, or open if it already exists
        const bool createAsZarr = true;
        try {
            z5::createFile(f, createAsZarr);
            //std::cout << "[database_zarr] created new file in zarr format" << std::endl;
        } catch (const std::invalid_argument& e) {
            // File already exists, that's okay - we'll just use the existing file
            //std::cout << "[database_zarr] using existing zarr file" << std::endl;
        }

        // Create datasets matching SQLite schema
        //std::vector<size_t> shape = {10000}; // Start with 10k entries
        //std::vector<size_t> chunks = {1000};
        //std::vector<size_t> jsonShape = {10000, 512}; // 512-byte strings for JSON
        //std::vector<size_t> jsonChunks = {1000, 512};

        // NEW: dataset shapes based on maxEntries / jsonWidth
        std::vector<size_t> shape      = {maxEntries};             // Start with maxEntries entries
        std::vector<size_t> chunks     = {1000};
        std::vector<size_t> jsonShape  = {maxEntries, jsonWidth};  // jsonWidth-byte strings
        std::vector<size_t> jsonChunks = {1000, jsonWidth};


    // Initialize in-memory buffers to match chunk layout
    idBuf    = xt::zeros<uint64_t>({chunkSize});
    stBuf    = xt::zeros<uint64_t>({chunkSize});
    dirBuf   = xt::zeros<int32_t> ({chunkSize});
    portBuf  = xt::zeros<uint64_t>({chunkSize});
    protoBuf = xt::zeros<int32_t> ({chunkSize});

    // jsonBuf is 2D: one row per transaction, jsonWidth bytes per row
    jsonBuf  = xt::zeros<uint8_t>({chunkSize, jsonWidth});

    bufCount     = 0;
    flushBaseIdx = 0;
    entryCount   = 0;   // if not already reset elsewhere





        // Create or open datasets using supported data types
        // id field (auto-incrementing integer)
        try {
            idDs = z5::createDataset(f, "id", "uint64", shape, chunks);
            fixZarrMetadata("data.zr/id/.zarray");
        } catch (const std::invalid_argument&) {
            idDs = z5::openDataset(f, "id");
        }

        // st field (timestamp)
        try {
            stDs = z5::createDataset(f, "st", "uint64", shape, chunks);
            fixZarrMetadata("data.zr/st/.zarray");
        } catch (const std::invalid_argument&) {
            stDs = z5::openDataset(f, "st");
        }

        // dir field (direction: 0=FW, 1=BW)
        try {
            dirDs = z5::createDataset(f, "dir", "int32", shape, chunks);
            fixZarrMetadata("data.zr/dir/.zarray");
        } catch (const std::invalid_argument&) {
            dirDs = z5::openDataset(f, "dir");
        }

        // port field (object id)
        try {
            portDs = z5::createDataset(f, "port", "uint64", shape, chunks);
            fixZarrMetadata("data.zr/port/.zarray");
        } catch (const std::invalid_argument&) {
            portDs = z5::openDataset(f, "port");
        }

        // proto field (protocol kind)
        try {
            protoDs = z5::createDataset(f, "proto", "int32", shape, chunks);
            fixZarrMetadata("data.zr/proto/.zarray");
        } catch (const std::invalid_argument&) {
            protoDs = z5::openDataset(f, "proto");
        }

        // json field (full JSON text)
        try {
            jsonDs = z5::createDataset(f, "json", "uint8", jsonShape, jsonChunks);
            fixZarrMetadata("data.zr/json/.zarray");
        } catch (const std::invalid_argument&) {
            jsonDs = z5::openDataset(f, "json");
        }

        //std::cout << "[database_zarr] opened/created datasets: id, st, dir, port, proto, json" << std::endl;
    }
void database_zarr::flushChunk()
{
    if (bufCount == 0)
        return;

    // We’re writing bufCount contiguous rows starting at flushBaseIdx
    z5::types::ShapeType offset   = {flushBaseIdx};
    z5::types::ShapeType jsonOff  = {flushBaseIdx, 0};

    // Use xt::view so we only write the valid prefix [0, bufCount)
    auto idView    = xt::view(idBuf,    xt::range<std::size_t>(0, bufCount));
    auto stView    = xt::view(stBuf,    xt::range<std::size_t>(0, bufCount));
    auto dirView   = xt::view(dirBuf,   xt::range<std::size_t>(0, bufCount));
    auto portView  = xt::view(portBuf,  xt::range<std::size_t>(0, bufCount));
    auto protoView = xt::view(protoBuf, xt::range<std::size_t>(0, bufCount));
    auto jsonView  = xt::view(jsonBuf,
                              xt::range<std::size_t>(0, bufCount),
                              xt::all());

    // Single-chunk writes instead of 1-element writes
    z5::multiarray::writeSubarray<uint64_t>(*idDs,    idView,    offset.begin());
    z5::multiarray::writeSubarray<uint64_t>(*stDs,    stView,    offset.begin());
    z5::multiarray::writeSubarray<int32_t> (*dirDs,   dirView,   offset.begin());
    z5::multiarray::writeSubarray<uint64_t>(*portDs,  portView,  offset.begin());
    z5::multiarray::writeSubarray<int32_t> (*protoDs, protoView, offset.begin());
    z5::multiarray::writeSubarray<uint8_t> (*jsonDs,  jsonView,  jsonOff.begin());

    flushBaseIdx += bufCount;
    bufCount = 0;
}






    void database_zarr::gen_meta(const meta_info &info)
    {
        //std::cout << "[database_zarr] meta: pid=" << info.pid
                  //<< " path=" << info.path
                  //<< " user=" << info.user
                  //<< " version=" << info.version
                  //<< " time=" << info.timestamp << std::endl;
    }

    void database_zarr::transaction_trace_fw(id_t obj, sysc_time_t st, protocol_kind proto, const char *json)
    {
        unsigned long long txn_id = unique_transaction_id.fetch_add(1, std::memory_order_relaxed);
        unique_fw_transaction_id.fetch_add(1, std::memory_order_relaxed);

        storeTransactionData(obj, st, proto, json, 0); // 0 = FW direction


        // Instrument for SystemC paper metrics
        if (txn_id % DB_INSTRUMENT_SIZE == 0){
            auto now = std::chrono::steady_clock::now();
            auto ms =  std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
            std::cerr << "TX_CHECKOUT " << txn_id << " transactions " <<  ms << " ms" << std::endl   << std::flush;
        }


    }

    void database_zarr::transaction_trace_bw(id_t obj, sysc_time_t st, protocol_kind proto, const char *json)
    {
    unsigned long long txn_id = unique_transaction_id.fetch_add(1, std::memory_order_relaxed);
    unique_bw_transaction_id.fetch_add(1, std::memory_order_relaxed);

    
        storeTransactionData(obj, st, proto, json, 1); // 1 = BW direction
    
        // Instrument for SystemC paper metrics
        if (txn_id % DB_INSTRUMENT_SIZE == 0){
            auto now = std::chrono::steady_clock::now();
            auto ms =  std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
            std::cerr << "TX_CHECKOUT " << txn_id << " transactions "  << ms << " ms" << std::endl   << std::flush;
        }

    
    }



void database_zarr::storeTransactionData(id_t obj,
                                         sysc_time_t timestamp,
                                         protocol_kind proto,
                                         const char* json,
                                         int direction)
{
    // Prevent overflow
    if (flushBaseIdx + bufCount >= maxEntries) {
        std::cerr << "[database_zarr] Warning: Dataset full, cannot store more entries "
                  << "(entryCount=" << entryCount
                  << ", maxEntries=" << maxEntries << ")\n";
        return;
    }

    // Index within the current in-memory chunk
    const std::size_t idx = bufCount;

    // Fill scalar fields in the buffer
    const std::uint64_t logicalId = flushBaseIdx + idx;   // same 0..N-1 scheme as before

    idBuf(idx)    = logicalId;
    stBuf(idx)    = static_cast<std::uint64_t>(timestamp);
    dirBuf(idx)   = direction;
    portBuf(idx)  = static_cast<std::uint64_t>(obj);
    protoBuf(idx) = static_cast<std::int32_t>(proto);

    // Copy JSON string into fixed-width row
    std::string jsonStr = (json && std::strlen(json) > 0) ? json : "";

    if (jsonStr.size() >= jsonWidth) {
        // keep room for an explicit '\0' if you care
        jsonStr.resize(jsonWidth - 1);
    }
    jsonStr.resize(jsonWidth, '\0');   // pad / null-terminate within the row

    for (std::size_t i = 0; i < jsonWidth; ++i) {
        jsonBuf(idx, i) = static_cast<std::uint8_t>(jsonStr[i]);
    }

    ++bufCount;
    ++entryCount;

    // When buffer is full, flush to disk
    if (bufCount == chunkSize) {
        flushChunk();
    }
}





/*
    void database_zarr::storeTransactionData(id_t obj, sysc_time_t timestamp, protocol_kind proto, const char* json, int direction)
    {
        //if (entryCount >= 10000) {
            //std::cerr << "[database_zarr] Warning: Dataset full, cannot store more entries" << std::endl;
            //return;
        //}

        if (entryCount >= maxEntries) {
            std::cerr << "[database_zarr] Warning: Dataset full, cannot store more entries "
                      << "(entryCount=" << entryCount
                      << ", maxEntries=" << maxEntries << ")\n";
            return;
        }



        try {
            // Prepare offset for all scalar fields
            z5::types::ShapeType offset = {entryCount};
            z5::types::ShapeType jsonOffset = {entryCount, 0};

            // Create arrays for each field
            xt::xarray<uint64_t> idArray = xt::zeros<uint64_t>({1});
            xt::xarray<uint64_t> stArray = xt::zeros<uint64_t>({1});
            xt::xarray<int32_t> dirArray = xt::zeros<int32_t>({1});
            xt::xarray<uint64_t> portArray = xt::zeros<uint64_t>({1});
            xt::xarray<int32_t> protoArray = xt::zeros<int32_t>({1});
            std::array<std::size_t, 2> jsonShape = {std::size_t(1), jsonWidth};
	    xt::xarray<uint8_t>  jsonArray  = xt::zeros<uint8_t>(jsonShape);

            // Fill in the data
            idArray[0] = entryCount;  // Auto-incrementing ID
            stArray[0] = static_cast<uint64_t>(timestamp);
            dirArray[0] = direction;
            portArray[0] = static_cast<uint64_t>(obj);
            protoArray[0] = static_cast<int32_t>(proto);

            // Copy JSON string (or empty string if null)
            std::string jsonStr = (json && strlen(json) > 0) ? json : "";
            //jsonStr.resize(511, '\0'); // Ensure null termination within 512 bytes
            //for (size_t i = 0; i < jsonStr.size() && i < 512; ++i) {
                //jsonArray(0, i) = static_cast<uint8_t>(jsonStr[i]);
            //}


            if (jsonStr.size() >= jsonWidth) {
                // leave room for an explicit '\0' if you care
                jsonStr.resize(jsonWidth - 1);
            }
            jsonStr.resize(jsonWidth, '\0'); // pad / null-terminate in the buffer

            for (size_t i = 0; i < jsonWidth; ++i) {
                jsonArray(0, i) = static_cast<uint8_t>(jsonStr[i]);
            }





            // Write all fields
            z5::multiarray::writeSubarray<uint64_t>(*idDs, idArray, offset.begin());
            z5::multiarray::writeSubarray<uint64_t>(*stDs, stArray, offset.begin());
            z5::multiarray::writeSubarray<int32_t>(*dirDs, dirArray, offset.begin());
            z5::multiarray::writeSubarray<uint64_t>(*portDs, portArray, offset.begin());
            z5::multiarray::writeSubarray<int32_t>(*protoDs, protoArray, offset.begin());
            z5::multiarray::writeSubarray<uint8_t>(*jsonDs, jsonArray, jsonOffset.begin());

            //std::cout << "[database_zarr] " << (direction == 0 ? "FW" : "BW") << " stored entry " << entryCount
                      //<< ": id=" << entryCount << " st=" << timestamp << " dir=" << direction
                      //<< " port=" << obj << " proto=" << proto << std::endl;

            entryCount++;
        } catch (const std::exception& e) {
            std::cerr << "[database_zarr] Error storing transaction data: " << e.what() << std::endl;
        }
    }

*/


    void database_zarr::retrieveTransactionData(size_t index)
    {
        if (index >= entryCount) {
            std::cerr << "[database_zarr] Index " << index << " out of range (0-" << entryCount-1 << ")" << std::endl;
            return;
        }

        try {
            z5::types::ShapeType offset = {index};
            z5::types::ShapeType jsonOffset = {index, 0};

            // Create arrays to read into
            xt::xarray<uint64_t> idArray = xt::zeros<uint64_t>({1});
            xt::xarray<uint64_t> stArray = xt::zeros<uint64_t>({1});
            xt::xarray<int32_t> dirArray = xt::zeros<int32_t>({1});
            xt::xarray<uint64_t> portArray = xt::zeros<uint64_t>({1});
            xt::xarray<int32_t> protoArray = xt::zeros<int32_t>({1});
            std::array<std::size_t, 2> jsonShape = {std::size_t(1), jsonWidth};
            xt::xarray<uint8_t>  jsonArray  = xt::zeros<uint8_t>(jsonShape);

            // Read all fields
            z5::multiarray::readSubarray<uint64_t>(*idDs, idArray, offset.begin());
            z5::multiarray::readSubarray<uint64_t>(*stDs, stArray, offset.begin());
            z5::multiarray::readSubarray<int32_t>(*dirDs, dirArray, offset.begin());
            z5::multiarray::readSubarray<uint64_t>(*portDs, portArray, offset.begin());
            z5::multiarray::readSubarray<int32_t>(*protoDs, protoArray, offset.begin());
            z5::multiarray::readSubarray<uint8_t>(*jsonDs, jsonArray, jsonOffset.begin());

            // Convert JSON bytes back to string
            std::string jsonStr(reinterpret_cast<const char*>(jsonArray.data()), 512);
            auto jsonNull = jsonStr.find('\0');
            if (jsonNull != std::string::npos) jsonStr = jsonStr.substr(0, jsonNull);

            std::cout << "[database_zarr] Retrieved entry " << index << ":" << std::endl;
            std::cout << "  id=" << idArray[0] << std::endl;
            std::cout << "  st=" << stArray[0] << std::endl;
            std::cout << "  dir=" << dirArray[0] << " (" << (dirArray[0] == 0 ? "FW" : "BW") << ")" << std::endl;
            std::cout << "  port=" << portArray[0] << std::endl;
            std::cout << "  proto=" << protoArray[0] << std::endl;
            std::cout << "  json=" << jsonStr << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "[database_zarr] Error retrieving transaction data: " << e.what() << std::endl;
        }
    }

    database_zarr::~database_zarr()
    {
        std::cout << "[database_zarr] destructor" << std::endl;
       flushChunk();

	    unsigned long long txn_id = unique_transaction_id.fetch_add(1, std::memory_order_relaxed);
    unique_fw_transaction_id.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "TX_CHECKOUT Number of fw transactions " << unique_fw_transaction_id.fetch_add(1, std::memory_order_relaxed) - 1 << std::endl   << std::flush;
    std::cerr << "TX_CHECKOUT Number of bw transactions " << unique_bw_transaction_id.fetch_add(1, std::memory_order_relaxed) - 1 << std::endl   << std::flush;
    std::cerr << "TX_CHECKOUT Number of transactions " << unique_transaction_id.fetch_add(1, std::memory_order_relaxed) - 1 << std::endl   << std::flush;
    stop();


    }

} // namespace inscight
  //
