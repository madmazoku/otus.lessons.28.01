#pragma once

#include "map_reduce.h"

class MRCopy : public IMapReduce {
public:
    virtual ~MRCopy() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) final {}
    virtual void done() final {}

    virtual void parse(const std::string& line, IKVWriter* kvw) final {
        kvw->put(KeyValue(line));
    }
    virtual void map(IKVReader* kvr, IKVWriter* kvw) final {
        kvw->put(kvr->get());
    };
    virtual void reduce(IKVReader* kvr, IKVWriter* kvw) final {
        kvw->put(kvr->get());
    }
    virtual std::string collect(KeyValue& kv) final {
        return kv._key;
    }
};

class MRCount : public IMapReduce {
public:
    virtual ~MRCount() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) final {}
    virtual void done() final {}

    virtual void parse(const std::string& line, IKVWriter* kvw) final {
        kvw->put(KeyValue(line));
    }
    virtual void map(IKVReader* kvr, IKVWriter* kvw) final {
        kvw->put(kvr->get());
    }
    virtual void reduce(IKVReader* kvr, IKVWriter* kvw) final {
        std::string key;
        size_t cnt = 0;
        while(!kvr->eof()) {
            KeyValue kv = kvr->get();
            if(key.empty() || key != kv._key) {
                if(!key.empty())
                    kvw->put(KeyValue(key, std::to_string(cnt)));
                key = kv._key;
                cnt = 1;
            }
        }
        if(!key.empty())
            kvw->put(KeyValue(key, std::to_string(cnt)));
    }
    virtual std::string collect(KeyValue& kv) final {
        return kv._key + "\t" + kv._value;
    }
};
