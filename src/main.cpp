
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

    std::cout << "src: " << config._src << std::endl;
    std::cout << "dst: " << config._dst << std::endl;
    std::cout << "tmp: " << config._tmp << std::endl;
    std::cout << "mappers: " << config._mappers << std::endl;
    std::cout << "reducers: " << config._reducers << std::endl;
    std::cout << "stages: " << config._mapreduce.size() << std::endl;

    FileRanges frs = split_file(config._src, config._mappers);
    std::cout << "split " << config._src << " into " << config._mappers << " parts" << std::endl;
    for(auto fr : frs) {
        std::cout << fr._start << " -> " << fr._end << " : " << (fr._end - fr._start) << std::endl;
    }

    std::mutex cout_mutex;

    Pipe<KeyValue> pipe;
    std::vector<std::unique_ptr<Pipe<KeyValue>>> pipes;

    for(size_t n = 0; n < config._reducers; ++n)
        pipes.push_back(std::make_unique<Pipe<KeyValue>>(10));

    std::vector<std::thread> reducers;
    std::vector<std::thread> mappers;

    for(auto fr : frs) {
        mappers.push_back(
            std::thread([fr, &config, &pipes, &cout_mutex]() {
                std::ifstream in(config._src, std::ifstream::binary);
                in.seekg(fr._start);
                std::string line;
                std::hash<std::string> hash_fn;
                while(in.tellg() < fr._end && std::getline(in, line)) {
                    for(size_t n = 1; n < line.size(); ++n) {
                        KeyValue kv(line.substr(0, n), line);
                        size_t reducer = hash_fn(kv._key) % config._reducers;
                        pipes[reducer]->put(kv);
                    }
                }
            })
        );
    }


    for(size_t n = 0; n < config._reducers; ++n) {
        reducers.push_back(
            std::thread([n, &config, &pipes, &pipe, &cout_mutex]() {
                KeyValue kv;
                std::vector<KeyValue> data;
                while(pipes[n]->get(kv)) 
                    data.push_back(kv);

                std::sort(data.begin(), data.end(), [](const KeyValue& a, const KeyValue& b){ return a._key < b._key; });

                KeyValue kvc;
                size_t count = 0;
                for(auto kv : data) {
                    if(count == 0 || kvc._key != kv._key) {
                        if(count == 1)
                            pipe.put(KeyValue(kvc._value, kvc._key));
                        kvc = kv;
                        count = 0;
                    }
                    ++count;
                }
                if(count == 1)
                    pipe.put(KeyValue(kvc._value, kvc._key));

            })
        );
    }

    std::thread collector([&config, &pipe]() {
        std::ofstream out(config._dst);
        KeyValue kv;
        size_t cnt = 0;
        while(pipe.get(kv)) {
            out << kv._key;
            if(!kv._value.empty()) 
                out << "\t" << kv._value;
            out << '\n';
        }
    });


    for(size_t n = 0; n < config._mappers; ++n)
        mappers[n].join();

    for(size_t n = 0; n < config._reducers; ++n) {
        pipes[n]->finish();
        reducers[n].join();
    }

    pipe.finish();
    collector.join();
}


/*
            // std::this_thread::sleep_for(std::chrono::milliseconds(20));

int main(int argc, char** argv)
{
    Script script;
    script.init();
    script.load(argv[1]);

    std::vector<std::tuple<std::string, std::string>> key_value;

    std::string line;
    while(std::getline(std::cin, line)) {
        std::string key, value;
        script.parse(line, key, value);
        key_value.push_back(std::make_tuple(key, value));
        std::cout << "c++ in   > key: " << key << "; value: " << value << std::endl;
    }

    std::sort(key_value.begin(), key_value.end(), [](auto a, auto b) { return std::get<0>(a) < std::get<0>(b); });

    std::vector<std::tuple<std::string, std::vector<std::string>>> key_values;
    auto it_kv_curr = key_value.end();
    for(auto it_kv = key_value.begin(); it_kv != key_value.end(); ++it_kv) {
        std::cout << "c++ part > key: " << std::get<0>(*it_kv) << "; value: " << std::get<1>(*it_kv) << std::endl;
        if(it_kv_curr == key_value.end() || std::get<0>(*it_kv_curr) != std::get<0>(*it_kv)) {
            key_values.push_back(std::make_tuple(std::get<0>(*it_kv), std::vector<std::string>{std::get<1>(*it_kv)}));
            it_kv_curr = it_kv;
        } else {
            std::get<1>(key_values.back()).push_back(std::get<1>(*it_kv));
        }
    }

    for(auto kvs : key_values) {
        std::cout << "c++ out  > key: " << std::get<0>(kvs) << std::endl;
        for(auto v : std::get<1>(kvs)) {
            std::cout << "\t\t" << v << std::endl;
            std::string line;
        }
        std::vector<std::string> lines;
        script.collect(lines, std::get<0>(kvs), std::get<1>(kvs));
        for(auto l : lines) {
            std::cout << l << std::endl;
        }
    }

    script.done();

    return 0;
}
*/