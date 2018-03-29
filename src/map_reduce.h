#pragma once

#include <boost/property_tree/ptree.hpp>
#include <vector>
#include <thread>

#include "key_value.h"

using KeyValues = std::vector<KeyValue>;

class IMapReduce
{
public:
    virtual ~IMapReduce() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) = 0;
    virtual void done() = 0;

    virtual void parse(const std::string& line, KeyValues& kvs_out) = 0;
    virtual void collect(const KeyValue& kv, std::string& line) = 0;

    virtual void map(const KeyValues& kvs_in, KeyValues& kvs_out) = 0;
    virtual void reduce(const KeyValues& kvs_in, KeyValues& kvs_out) = 0;
};

class MR_count : public IMapReduce
{
public:
    virtual ~MR_count() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) final {}
    virtual void done() final {}

    virtual void parse(const std::string& line, KeyValues& kvs_out) final {
        kvs_out.push_back(KeyValue(line));
    }
    virtual void map(const KeyValues& kvs_in, KeyValues& kvs_out) final {
        kvs_out = kvs_in;
    }

    virtual void reduce(const KeyValues& kvs_in, KeyValues& kvs_out) final {
        kvs_out.push_back(KeyValue(kvs_in.front()._key, std::to_string(kvs_in.size())));
    }
    virtual void collect(const KeyValue& kv, std::string& line) final {
        line = kv._key + "\t" + kv._value;
    }
};

class MR_uniq_shorts : public IMapReduce
{
public:
    virtual ~MR_uniq_shorts() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) final {}
    virtual void done() final {}

    virtual void parse(const std::string& line, KeyValues& kvs_out) final {
        kvs_out.push_back(KeyValue(line));
    }
    virtual void map(const KeyValues& kvs_in, KeyValues& kvs_out) final {
        for(auto& kv : kvs_in)
            for(size_t n = 1; n <= kv._key.size(); ++n)
                kvs_out.push_back(KeyValue(kv._key.substr(0, n), kv._key));
    }

    virtual void reduce(const KeyValues& kvs_in, KeyValues& kvs_out) final {
        if(kvs_in.size() == 1)
            kvs_out.push_back(KeyValue(kvs_in.front()._value, kvs_in.front()._key));
    }
    virtual void collect(const KeyValue& kv, std::string& line) final {
        line = kv._key + "\t" + kv._value;
    }
};

class MR_shortest : public IMapReduce
{
public:
    virtual ~MR_shortest() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) final {}
    virtual void done() final {}

    virtual void parse(const std::string& line, KeyValues& kvs_out) final {
        size_t npos = line.find('\t');
        kvs_out.push_back(KeyValue(line.substr(0, npos), line.substr(npos + 1)));
    }
    virtual void map(const KeyValues& kvs_in, KeyValues& kvs_out) final {
        kvs_out = kvs_in;
    }

    virtual void reduce(const KeyValues& kvs_in, KeyValues& kvs_out) final {
        auto it_s = kvs_in.begin();
        auto it = it_s;
        while(++it != kvs_in.end())
            if(it->_value.size() < it_s->_value.size())
                it_s = it;
        kvs_out.push_back(*it_s);
    }
    virtual void collect(const KeyValue& kv, std::string& line) final {
        line = kv._key + "\t" + kv._value;
    }
};
