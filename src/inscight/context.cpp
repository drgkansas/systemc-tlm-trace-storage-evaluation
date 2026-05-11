/******************************************************************************
 *                                                                            *
 * Copyright 2023 MachineWare GmbH                                            *
 * All Rights Reserved                                                        *
 *                                                                            *
 * This is unpublished proprietary work owned by MachineWare GmbH. It may be  *
 * used, modified and distributed in accordance to the license specified by   *
 * the license file in the root directory of this project.                    *
 *                                                                            *
 ******************************************************************************/

#include "inscight/context.h"
#include "inscight/database.h"
#include "inscight/database_csv.h"
#include "inscight/database_sql.h"
#include "inscight/database_parquet.h"
#include "inscight/database_zarr.h"

namespace inscight {

static database* create_database(const std::string& options) {

    // Consider adding a mode that collapses all of the CSV data into one file
    if (options.find("csv") != std::string::npos)
        return new database_csv(options);
    else if (options.find("parquet") != std::string::npos)
        return new database_parquet(options);
    else if (options.find("zarr") != std::string::npos)
        return new database_zarr(options);
    else
        // Sqlite Format
        return new database_sql(options);

    // Exotic Simulation Data Formats
    /*
    else if (options.find("sqlite") != std::string::npos)
        return new database_sql(options);
    else if (options.find("postgres") != std::string::npos)
        return new database_postgress(options);
    else if (options.find("netcdf") != std::string::npos)
        return new database_netcdf(options);
    else if (options.find("zaar") != std::string::npos)
        return new database_zaar(options);
    else if (options.find("mongodb") != std::string::npos)
        return new database_mongodb(options);
    else if (options.find("wireshark") != std::string::npos)
        return new database_wireshark(options);
    */

}

context::context(const std::string& options):
    m_db(create_database(options)) {
    m_db->start();
}

context::~context() {
    delete m_db;
}

static context* init() {
    const char* str = getenv("INSCIGHT");
    if (str == nullptr || strcmp(str, "0") == 0)
        return nullptr;
    static context singleton(str);
    return &singleton;
}

context* ctx = init();

} // namespace inscight
