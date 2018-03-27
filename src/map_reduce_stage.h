#pragma once

#include <thread>

#include "map_reduce.h"
#include "utils.h"

class MapReduceStage {
private:
    IMapReduce* _mr;
    FileRange _fr;
    IKVWrite* _kvw;

public:
    MapReduceStage(IMapReduce* mr, FileRange fr, IKVWrite* kvw) : _mr(mr), _fr(fr), _kvw(kvw) { }

    void map() {
        ifstream in(_fr._file_name);
        in.seekg(_fr.start);
        std::string line;
        while(in.tellg() < _fr.end && std::getline(in, line))
            _mr->map(line, _kvw);
    }
}