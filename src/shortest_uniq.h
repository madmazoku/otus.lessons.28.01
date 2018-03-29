#pragma once

#include "block.h"

namespace shortest_uniq
{

class BP1m : public BProcess
{
public:
    BP1m(size_t channels) : BProcess(channels)
    {
    }

    virtual ~BP1m() = default;

    virtual void process(size_t n)
    {
        KeyValue kv;
        while(_ins[n]->_pipe.get(kv)) {
            for(size_t m = 1; m < kv._key.size(); ++m)
                (*_outs)[n]->_pipe.put(KeyValue(kv._key.substr(0, m), kv._key));
        }
    }
};

class BP2r : public BProcess
{
public:
    BP2r(size_t channels) : BProcess(channels)
    {
    }

    virtual ~BP2r() = default;

    virtual void process(size_t n)
    {
        KeyValue kv, kvc;
        size_t count = 0;
        while(_ins[n]->_pipe.get(kv))
            if(count == 0 || kvc._key != kv._key) {
                if(count == 1)
                    (*_outs)[n]->_pipe.put(KeyValue(kv._value, kv._key));
                kvc = kv;
                count = 1;
            } else
                ++count;
        if(count == 1)
            (*_outs)[n]->_pipe.put(KeyValue(kv._value, kv._key));
    }
};

class BP3r : public BProcess
{
public:
    BP3r(size_t channels) : BProcess(channels)
    {
    }

    virtual ~BP3r() = default;

    virtual void process(size_t n)
    {
        KeyValue kv, kvc;
        while(_ins[n]->_pipe.get(kv))
            if(kvc._key.empty() || kvc._key != kv._key) {
                if(!kvc._key.empty())
                    (*_outs)[n]->_pipe.put(kvc);
                kvc = kv;
            } else if(kvc._value.size() > kv._value.size())
                kvc = kv;
        if(!kvc._key.empty())
            (*_outs)[n]->_pipe.put(kvc);
    }
};

}