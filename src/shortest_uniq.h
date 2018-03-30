#pragma once

#include "block.h"

namespace shortest_uniq
{

class BP1m : public BProcess
{
public:
    BP1m(const std::string& tag, size_t channels) : BProcess(tag, channels)
    {
    }

    virtual ~BP1m() = default;

    virtual void process(size_t n, Pipe<KeyValue>& in, Pipe<KeyValue>& out)
    {
        size_t get_count = 0;
        size_t put_count = 0;
        KeyValue kv;
        while(in.get(kv)) {
            ++get_count;
            for(size_t m = 1; m <= kv._key.size(); ++m) {
                out.put(KeyValue(kv._key.substr(0, m), kv._key));
                ++put_count;
            }
        }
        update_metrics(n, get_count, put_count);
    }
};

class BP2r : public BProcess
{
public:
    BP2r(const std::string& tag, size_t channels) : BProcess(tag,channels)
    {
    }

    virtual ~BP2r() = default;

    virtual void process(size_t n, Pipe<KeyValue>& in, Pipe<KeyValue>& out)
    {
        size_t get_count = 0;
        size_t put_count = 0;
        KeyValue kv, kvc;
        size_t count = 0;
        while(in.get(kv)) {
            ++get_count;
            if(count == 0 || kvc._key != kv._key) {
                if(count == 1) {
                    out.put(KeyValue(kvc._value, kvc._key));
                    ++put_count;
                }
                kvc = kv;
                count = 1;
            } else
                ++count;
        }
        if(count == 1) {
            out.put(KeyValue(kvc._value, kvc._key));
            ++put_count;
        }
        update_metrics(n, get_count, put_count);
    }
};

class BP3r : public BProcess
{
public:
    BP3r(const std::string& tag, size_t channels) : BProcess(tag,channels)
    {
    }

    virtual ~BP3r() = default;

    virtual void process(size_t n, Pipe<KeyValue>& in, Pipe<KeyValue>& out)
    {
        size_t get_count = 0;
        size_t put_count = 0;
        KeyValue kv, kvc;
        while(in.get(kv)) {
            ++get_count;
            if(kvc._key.empty() || kvc._key != kv._key) {
                if(!kvc._key.empty()) {
                    out.put(KeyValue(kvc._value, kvc._key));
                    ++put_count;
                }
                kvc = kv;
            } else if(kvc._value.size() > kv._value.size())
                kvc = kv;
        }
        if(!kvc._key.empty()) {
            out.put(KeyValue(kvc._value, kvc._key));
            ++put_count;
        }
        update_metrics(n, get_count, put_count);
    }
};

}