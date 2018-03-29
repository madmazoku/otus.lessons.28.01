#pragma once

#include <string>
#include <ostream>

struct KeyValue {
    std::string _key;
    std::string _value;

    KeyValue() : _key(""), _value("") {}
    KeyValue(const std::string& key, const std::string& value = std::string{""}) : _key(key), _value(value) {}
    KeyValue(const KeyValue& kv) : _key(kv._key), _value(kv._value) {}
    KeyValue(KeyValue&& kv) : _key(std::move(kv._key)), _value(std::move(kv._value)) {}

    KeyValue& operator=(const KeyValue& kv)
    {
        _key = kv._key;
        _value = kv._value;
        return *this;
    }
    KeyValue& operator=(KeyValue&& kv)
    {
        std::swap(_key, kv._key);
        std::swap(_value, kv._value);
        return *this;
    }

    bool operator<(const KeyValue kv)
    {
        return _key < kv._key;
    }
};

std::ostream& operator<<(std::ostream& out, const KeyValue& kv )
{
    out << "{ '" << kv._key << "', '" << kv._value << "' }";
}

