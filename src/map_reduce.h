#pragma once

#include <boost/property_tree/ptree.hpp>

#include <string>

struct KeyValue {
    std::string _key;
    std::string _value;

    KeyValue() : _key(""), _value("") {}
    KeyValue(const std::string& key, const std::string& value = std::string{""}) : _key(key), _value(value) {}
    KeyValue(const KeyValue& kv) : _key(kv._key), _value(kv._value) {}
    KeyValue(KeyValue&& kv) : _key(std::move(kv._key)), _value(std::move(kv._value)) {}

    KeyValue& operator=(const KeyValue& kv) {
        _key = kv._key;
        _value = kv._value;
        return *this;
    }
    KeyValue& operator=(KeyValue&& kv) {
        std::swap(_key, kv._key);
        std::swap(_value, kv._value);
        return *this;
    }
};

class IKVReader {
public:
    virtual ~IKVReader() = default;

    virtual bool eof() = 0;
    virtual KeyValue get() = 0;
};

class IKVWriter {
public:
    virtual ~IKVWriter() = default;

    virtual void put(const KeyValue&) = 0;
};

class IMapReduce {
public:
    virtual ~IMapReduce() = default;

    virtual void init(const boost::property_tree::ptree& mapreduce) = 0;
    virtual void done() = 0;

    virtual void parse(const std::string& line, IKVWriter* kvw) = 0;
    virtual void map(IKVReader* kvr, IKVWriter* kvw, size_t stage) = 0;
    virtual void reduce(IKVReader* kvr, IKVWriter* kvw, size_t stage) = 0;
    virtual std::string collect(KeyValue& kv) = 0;
};
