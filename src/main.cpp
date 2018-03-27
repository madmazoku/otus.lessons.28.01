
#include <iostream>
#include "../bin/version.h"

#include <tuple>
#include <algorithm>

#include "utils.h"
#include "config.h"
#include "script.h"

#include "pipe.h"

#include <thread>
#include <functional>
#include <memory>

#include "map_reduce_stage.h"

int main(int argc, char** argv)
{
    if(argc < 2) {
        std::cout << "Usage " << argv[0] << " config.json" << std::endl;
        std::cout << "\tWhere config.json have fields: " << std::endl;
        std::cout << "\t\tsrc - source data file, string, required" << std::endl;
        std::cout << "\t\tdst - destination data file, string, required" << std::endl;
        std::cout << "\t\ttmp - directory for temporary files, string, optional, default '/tmp'" << std::endl;
        std::cout << "\t\tmapreduce - setups for map-reduce code, array of objects, required" << std::endl;
        std::cout << "\t\t\ttype - type of map-reduce code, string, required" << std::endl;
        std::cout << "\t\tmappers - number of mapper threads, integer, optional, default 10" << std::endl;
        std::cout << "\t\treducers - number of reduce threads, integer, optional, default 10" << std::endl;
        return 0;
    }

    Config config(argv[1]);

    std::cout << "config:" << std::endl;
    std::cout << "\tsrc: " << config._src << std::endl;
    std::cout << "\tdst: " << config._dst << std::endl;
    std::cout << "\ttmp: " << config._tmp << std::endl;
    std::cout << "\tmappers: " << config._mappers << std::endl;
    std::cout << "\treducers: " << config._reducers << std::endl;
    std::cout << "\tstages: " << config._stages.size() << std::endl;

    for(size_t n = 0; n < config._stages.size(); ++n)
        map_reduce_stage(config, n);

    return 0;

}
