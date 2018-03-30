#pragma once

#include <memory>
#include <vector>
#include <string>
#include <fstream>
#include <exception>
#include <thread>

#include "pipe.h"
#include "key_value.h"
#include "utils.h"

class Setup
{
public:
    size_t _max_pipe_buffer_size;

    Setup() : _max_pipe_buffer_size(100) {}

    static Setup& get()
    {
        static Setup setup;
        return setup;
    }
};

template<typename T>
class Channel
{
public:
    Pipe<T> _pipe;
    std::thread _thread;

    Channel() : _pipe(Setup::get()._max_pipe_buffer_size) {}
    Channel(const Channel&) = delete;
    Channel(Channel&&) = default;
};

template<typename T>
using ChannelPtr = std::unique_ptr<Channel<T>>;

template<typename T>
using ChannelPtrs = std::vector<ChannelPtr<T>>;

template<typename TIn>
class BlockLast
{
public:
    using Self = BlockLast<TIn>;

    ChannelPtrs<TIn> _ins;

    BlockLast(size_t inputs)
    {
        for(size_t n = 0; n < inputs; ++n)
            _ins.push_back(std::make_unique<Channel<TIn>>());
    }

    virtual ~BlockLast() = default;

    virtual void process(size_t n) = 0;

    void run()
    {
        for(size_t n = 0; n < _ins.size(); ++n)
            _ins[n]->_thread = std::thread([this, n]() {
            process(n);
        });
    }
    void finish()
    {
        for(size_t n = 0; n < _ins.size(); ++n)
            _ins[n]->_pipe.finish();
    }
    void join()
    {
        for(size_t n = 0; n < _ins.size(); ++n)
            _ins[n]->_thread.join();
    }
};

template<typename TIn, typename TOut>
class Block : public BlockLast<TIn>
{
public:
    using Self = Block<TIn, TOut>;

    ChannelPtrs<TOut>* _outs;

    Block(size_t inputs) : BlockLast<TIn>(inputs), _outs(nullptr)
    {
    }

    virtual ~Block() = default;

    virtual void validate( ChannelPtrs<TOut>& outs) = 0;

    void attach(ChannelPtrs<TOut>& outs)
    {
        validate(outs);
        _outs = &outs;
    }

    void detach()
    {
        _outs = nullptr;
    }

    void done()
    {
        BlockLast<TIn>::finish();
        BlockLast<TIn>::join();
        detach();
    }
};

class BSplitFile : public Block<std::string, FileRange>
{
public:
    BSplitFile() : Self(1)
    {
    }

    virtual ~BSplitFile() = default;

    virtual void validate(ChannelPtrs<FileRange>& outs)
    {
    }

    virtual void process(size_t n)
    {
        std::string file_name;
        while(_ins[n]->_pipe.get(file_name)) {
            FileRanges frs = split_file(file_name, _outs->size());
            for(size_t n = 0; n < frs.size(); ++n)
                (*_outs)[n]->_pipe.put(frs[n]);
        }
    }
};

class BReadFile : public Block<FileRange, std::string>
{
public:
    BReadFile(size_t inputs) : Self(inputs)
    {
    }

    virtual ~BReadFile() = default;

    virtual void validate(ChannelPtrs<std::string>& outs)
    {
        if(_ins.size() != outs.size())
            throw std::runtime_error("ins and outs must have equal channels count for BReadFile");
    }

    virtual void process(size_t n)
    {
        FileRange fr;
        while(_ins[n]->_pipe.get(fr)) {
            std::ifstream in(fr._file_name, std::ifstream::binary);
            in.seekg(fr._start);

            std::string line;
            while(in.tellg() < fr._end && std::getline(in, line))
                (*_outs)[n]->_pipe.put(line);
        }
    }
};

class BConvertToKV : public Block<std::string, KeyValue>
{
public:
    BConvertToKV(size_t channels) : Self(channels)
    {
    }

    virtual ~BConvertToKV() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
        if(_ins.size() != outs.size())
            throw std::runtime_error("ins and outs must have equal channels count for BConvertToKV");
    }

    virtual void process(size_t n)
    {
        std::string line;
        while(_ins[n]->_pipe.get(line))
            (*_outs)[n]->_pipe.put(KeyValue(line));
    }
};

class BProcess : public Block<KeyValue, KeyValue>
{
public:
    BProcess(size_t channels) : Self(channels)
    {
    }

    virtual ~BProcess() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
        if(_ins.size() != outs.size())
            throw std::runtime_error("ins and outs must have equal channels count for BProcess");
    }

    virtual void process(size_t n)
    {
        process(n, _ins[n]->_pipe, (*_outs)[n]->_pipe);
    }

    virtual void process(size_t n, Pipe<KeyValue>& in, Pipe<KeyValue>& out) = 0;
};

class BShard : public Block<KeyValue, KeyValue>
{
public:
    BShard(size_t channels) : Self(channels)
    {
    }

    virtual ~BShard() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
    }

    virtual void process(size_t n)
    {
        std::hash<std::string> hash_fn;

        KeyValue kv;
        while(_ins[n]->_pipe.get(kv))
            (*_outs)[hash_fn(kv._key) % _outs->size()]->_pipe.put(kv);
    }
};

class BSort : public Block<KeyValue, KeyValue>
{
public:
    BSort(size_t channels) : Self(channels)
    {
    }

    virtual ~BSort() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
        if(_ins.size() != outs.size())
            throw std::runtime_error("ins and outs must have equal channels count for BSort");
    }

    virtual void process(size_t n)
    {
        KeyValues kvs;
        KeyValue kv;
        while(_ins[n]->_pipe.get(kv))
            kvs.push_back(kv);
        std::sort(kvs.begin(), kvs.end());
        for(auto it = kvs.begin(); it != kvs.end(); ++it)
            (*_outs)[n]->_pipe.put(*it);
    }
};

class BSequence : public Block<KeyValue, KeyValue>
{
public:
    BSequence(size_t channels) : Self(channels)
    {
    }

    virtual ~BSequence() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
    }

    virtual void process(size_t n)
    {
        std::hash<std::string> hash_fn;

        KeyValue kv;
        size_t cnt = 0;
        while(_ins[n]->_pipe.get(kv)) {
            (*_outs)[cnt]->_pipe.put(kv);
            if(++cnt == _outs->size())
                cnt = 0;
        }
    }
};

class BConvertFromKV : public Block<KeyValue, std::string>
{
public:
    BConvertFromKV(size_t channels) : Self(channels)
    {
    }

    virtual ~BConvertFromKV() = default;

    virtual void validate(ChannelPtrs<std::string>& outs)
    {
        if(_ins.size() != outs.size())
            throw std::runtime_error("ins and outs must have equal channels count for BConvertFromKV");
    }

    virtual void process(size_t n)
    {
        KeyValue kv;
        while(_ins[n]->_pipe.get(kv)) {
            std::string line = kv._key;
            if(!kv._value.empty())
                line += '\t' + kv._value;
            (*_outs)[n]->_pipe.put(line);
        }
    }
};

class BSink : public Block<std::string, std::string>
{
public:
    BSink(size_t channels) : Self(channels)
    {
    }

    virtual ~BSink() = default;

    virtual void validate(ChannelPtrs<std::string>& outs)
    {
        if(1 != outs.size())
            throw std::runtime_error("outs must have exactly 1 channel for BSink");
    }

    virtual void process(size_t n)
    {
        std::string line;
        while(_ins[n]->_pipe.get(line))
            (*_outs)[0]->_pipe.put(line);
    }

};

class BWriteFile : public BlockLast<std::string>
{
public:
    std::ostream& _out;
    BWriteFile(std::ostream& out) : Self(1), _out(out)
    {
    }

    virtual ~BWriteFile() = default;

    virtual void validate(ChannelPtrs<std::string>& outs)
    {
        if(1 != outs.size())
            throw std::runtime_error("outs must have exactly 0 channels for BWriteFile");
    }

    virtual void process(size_t n)
    {
        std::string line;
        while(_ins[n]->_pipe.get(line))
            _out << line << "\n";
    }
};
