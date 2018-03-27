#pragma once

#include <memory>
#include <vector>
#include <thread>

#include "utils.h"
#include "config.h"
#include "pipe.h"

using IMapReducePtr = std::unique_ptr<IMapReduce>;
using KVPipe = Pipe<KeyValue>;
using IKVPipePtr = std::unique_ptr<KVPipe>;
using IKVPipePtrs = std::vector<IKVPipePtr>;
using Threads = std::vector<std::thread>;

IMapReducePtr get_mr(const std::string& type) {
    if(type == "MR_count")
        return std::make_unique<MR_count>();
    else if(type == "MR_uniq_shorts")
        return std::make_unique<MR_uniq_shorts>();
    else if(type == "MR_shortest")
        return std::make_unique<MR_shortest>();
    else
        return nullptr;
}

void map_reduce_stage(const Config& config, size_t stage = 0) {
    std::string type = config._stages[stage].get<std::string>("type");
    IMapReducePtr mr = get_mr(type);
    if(mr == nullptr)
        return;

    std::cout << "stage # " << stage << "; use mapreduce type: " << type << std::endl;

    mr->init(config._stages[stage]);

    std::string src;
    if(stage == 0)
        src = config._src;
    else
        src = config._tmp + "/stage_" + std::to_string(stage) + ".txt";

    std::string dst;
    if(stage + 1 == config._stages.size())
        dst = config._dst;
    else
        dst = config._tmp + "/stage_" + std::to_string(stage + 1) + ".txt";

    std::cout << "\tsrc: " << src << std::endl;
    std::cout << "\tdst: " << dst << std::endl;

    FileRanges frs = split_file(src, config._mappers);

    std::cout << "\tfile ranges:" << std::endl;
    for(size_t n = 0; n < frs.size(); ++n)
        std::cout << "\t\t" << n << " : " << frs[n]._start << " -> " << frs[n]._end << " : " << (frs[n]._end - frs[n]._start) << std::endl;

    Threads mappers;
    Threads reducers;
    IKVPipePtrs pipes;
    IKVPipePtr pipe = std::make_unique<KVPipe>(config._pipe_size);

    for(size_t n = 0; n < config._reducers; ++n)
        pipes.push_back(std::make_unique<KVPipe>(config._pipe_size));

    for(auto fr : frs) {
        mappers.push_back(
            std::thread([fr, &mr, &src, &config, &pipes]() {
                std::ifstream in(src, std::ifstream::binary);
                in.seekg(fr._start);

                std::string line;
                std::hash<std::string> hash_fn;
                KeyValues kvs_in, kvs_out;

                while(in.tellg() < fr._end && std::getline(in, line)) {
                    kvs_in.clear();
                    kvs_out.clear();

                    mr->parse(line, kvs_in);
                    mr->map(kvs_in, kvs_out);

                    for(auto& kv : kvs_out) {
                        size_t reducer = hash_fn(kv._key) % config._reducers;
                        pipes[reducer]->put(kv);
                    }

                }
            })
        );
    }

    for(size_t n = 0; n < config._reducers; ++n) {
        reducers.push_back(
            std::thread([n, &mr, &config, &pipes, &pipe]() {
                KeyValue kv;
                std::vector<KeyValue> data;
                while(pipes[n]->get(kv))
                    data.push_back(kv);

                std::sort(data.begin(), data.end(), [](const KeyValue& a, const KeyValue& b){ return a._key < b._key; });

                KeyValues kvs_in, kvs_out;

                auto it_start = data.begin();
                while(it_start != data.end()) {
                    auto it_end = it_start;
                    while(++it_end != data.end())
                        if(it_start->_key != it_end->_key)
                            break;

                    kvs_in.clear();
                    kvs_out.clear();
                    
                    std::copy(it_start, it_end, std::back_inserter(kvs_in));
                    it_start = it_end;

                    mr->reduce(kvs_in, kvs_out);

                    for(auto& kv : kvs_out)
                        pipe->put(kv);
                }

            })
        );
    }

    std::thread collector([&dst, &mr, &config, &pipe]() {
        std::ofstream out(dst);
        KeyValue kv;
        std::string line;
        while(pipe->get(kv)) {
            mr->collect(kv, line);
            out << line << "\n";
        }
    });

    for(size_t n = 0; n < config._mappers; ++n)
        mappers[n].join();

    for(size_t n = 0; n < config._reducers; ++n) {
        pipes[n]->finish();
        reducers[n].join();
    }

    pipe->finish();
    collector.join();

    mr->done();
}