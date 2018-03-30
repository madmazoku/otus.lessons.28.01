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
#include "metrics.h"

// Travis do not have it
template<typename T, typename... Args>
std::unique_ptr<T> make_unique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}


class Setup
{
public:
    size_t _max_pipe_buffer_size;
    Metrics _metrics;

    Setup() : _max_pipe_buffer_size(100) {}
    ~Setup()
    {
        std::ofstream out("metrics.txt");
        _metrics.dump("ymr", out);
    }

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

    std::string _tag;
    ChannelPtrs<TIn> _ins;

    BlockLast(const std::string tag, size_t inputs) : _tag(tag)
    {
        for(size_t n = 0; n < inputs; ++n)
            _ins.push_back(make_unique<Channel<TIn>>());
    }

    virtual ~BlockLast() = default;

    virtual void process(size_t n) = 0;

    void run()
    {
        for(size_t n = 0; n < _ins.size(); ++n) {
            _ins[n]->_thread = std::thread([this, n]() {
                process(n);
            });
        }
    }
    void finish()
    {
        metrics_t m;
        size_t writed = 0;
        for(size_t n = 0; n < _ins.size(); ++n) {
            _ins[n]->_pipe.finish();
            size_t writed_n = _ins[n]->_pipe.put_count();
            writed += writed_n;
            m[_tag + "." + std::to_string(n) + ".writed"] = writed_n;
        }
        m[_tag + ".writed"] = writed;
        Setup::get()._metrics.update(m);
    }
    void join()
    {
        metrics_t m;
        size_t readed = 0;
        for(size_t n = 0; n < _ins.size(); ++n) {
            _ins[n]->_thread.join();
            size_t readed_n = _ins[n]->_pipe.get_count();
            readed += readed_n;
            m[_tag + "." + std::to_string(n) + ".readed"] = readed_n;
        }
        m[_tag + ".readed"] = readed;
        Setup::get()._metrics.update(m);
    }
};

template<typename TIn, typename TOut>
class Block : public BlockLast<TIn>
{
public:
    using Self = Block<TIn, TOut>;

    ChannelPtrs<TOut>* _outs;

    Block(const std::string tag, size_t inputs) : BlockLast<TIn>(tag, inputs), _outs(nullptr)
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

    void update_metrics(size_t n, size_t get_count, size_t put_count)
    {
        if(!BlockLast<TIn>::_tag.empty()) {
            metrics_t m;
            std::string prefix = BlockLast<TIn>::_tag;
            m[prefix + ".get_count"] = get_count;
            m[prefix + ".put_count"] = put_count;
            prefix += "." + std::to_string(n);
            m[prefix + ".get_count"] = get_count;
            m[prefix + ".put_count"] = put_count;
            Setup::get()._metrics.update(m);
        }
    }
};

class BSplitFile : public Block<std::string, FileRange>
{
public:
    BSplitFile(const std::string tag) : Self(tag, 1)
    {
    }

    virtual ~BSplitFile() = default;

    virtual void validate(ChannelPtrs<FileRange>& outs)
    {
    }

    virtual void process(size_t n)
    {
        size_t get_count = 0;
        size_t put_count = 0;
        std::string file_name;
        while(_ins[n]->_pipe.get(file_name)) {
            ++get_count;
            FileRanges frs = split_file(file_name, _outs->size());
            for(size_t n = 0; n < frs.size(); ++n) {
                (*_outs)[n]->_pipe.put(frs[n]);
                ++put_count;
            }
        }
        update_metrics(n, get_count, put_count);
    }
};

class BReadFile : public Block<FileRange, std::string>
{
public:
    BReadFile(const std::string tag, size_t inputs) : Self(tag, inputs)
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
        size_t get_count = 0;
        size_t put_count = 0;
        FileRange fr;
        while(_ins[n]->_pipe.get(fr)) {

            std::ifstream in(fr._file_name, std::ifstream::binary);
            in.seekg(fr._start);

            std::string line;
            while(in.tellg() < fr._end && std::getline(in, line)) {
                (*_outs)[n]->_pipe.put(line);
                ++put_count;
            }
        }
        update_metrics(n, get_count, put_count);
    }
};

class BConvertToKV : public Block<std::string, KeyValue>
{
public:
    BConvertToKV(const std::string tag, size_t channels) : Self(tag, channels)
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
        size_t get_count = 0;
        size_t put_count = 0;
        std::string line;
        while(_ins[n]->_pipe.get(line)) {
            ++get_count;
            (*_outs)[n]->_pipe.put(KeyValue(line));
            ++put_count;
        }
        update_metrics(n, get_count, put_count);
    }
};

class BProcess : public Block<KeyValue, KeyValue>
{
public:
    BProcess(const std::string tag, size_t channels) : Self(tag, channels)
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
    BShard(const std::string tag, size_t channels) : Self(tag, channels)
    {
    }

    virtual ~BShard() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
    }

    virtual void process(size_t n)
    {
        std::hash<std::string> hash_fn;

        size_t get_count = 0;
        size_t put_count = 0;
        std::vector<size_t> put_count_m(_outs->size(), 0);

        KeyValue kv;
        while(_ins[n]->_pipe.get(kv)) {
            ++get_count;
            size_t m = hash_fn(kv._key) % _outs->size();
            (*_outs)[m]->_pipe.put(kv);
            ++put_count;
            ++put_count_m[m];
        }
        update_metrics(n, get_count, put_count);

        if(!_tag.empty()) {
            metrics_t m;

            for(size_t i = 0; i < put_count_m.size(); ++i)
                m[_tag + ".to." + std::to_string(i) + ".put_count"] = put_count_m[i];

            std::string prefix = _tag + "." + std::to_string(n);
            for(size_t i = 0; i < put_count_m.size(); ++i)
                m[prefix + ".to." + std::to_string(i) + ".put_count"] = put_count_m[i];

            Setup::get()._metrics.update(m);
        }
    }
};

class BSort : public Block<KeyValue, KeyValue>
{
public:
    BSort(const std::string tag, size_t channels) : Self(tag, channels)
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
        size_t get_count = 0;
        size_t put_count = 0;
        KeyValues kvs;
        KeyValue kv;
        while(_ins[n]->_pipe.get(kv)) {
            ++get_count;
            kvs.push_back(kv);
        }
        std::sort(kvs.begin(), kvs.end());
        for(auto it = kvs.begin(); it != kvs.end(); ++it) {
            (*_outs)[n]->_pipe.put(*it);
            ++put_count;
        }
        update_metrics(n, get_count, put_count);
    }
};

class BSequence : public Block<KeyValue, KeyValue>
{
public:
    BSequence(const std::string tag, size_t channels) : Self(tag, channels)
    {
    }

    virtual ~BSequence() = default;

    virtual void validate(ChannelPtrs<KeyValue>& outs)
    {
    }

    virtual void process(size_t n)
    {
        std::hash<std::string> hash_fn;

        size_t get_count = 0;
        size_t put_count = 0;
        std::vector<size_t> put_count_m(_outs->size(), 0);

        KeyValue kv;
        size_t m = 0;
        while(_ins[n]->_pipe.get(kv)) {
            ++get_count;
            (*_outs)[m]->_pipe.put(kv);
            ++put_count;
            ++put_count_m[m];

            if(++m == _outs->size())
                m = 0;
        }
        update_metrics(n, get_count, put_count);

        if(!_tag.empty()) {
            metrics_t m;

            for(size_t i = 0; i < put_count_m.size(); ++i)
                m[_tag + ".to." + std::to_string(i) + ".put_count"] = put_count_m[i];

            std::string prefix = _tag + "." + std::to_string(n);
            for(size_t i = 0; i < put_count_m.size(); ++i)
                m[prefix + ".to." + std::to_string(i) + ".put_count"] = put_count_m[i];

            Setup::get()._metrics.update(m);
        }
    }
};

class BConvertFromKV : public Block<KeyValue, std::string>
{
public:
    BConvertFromKV(const std::string tag, size_t channels) : Self(tag, channels)
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
        size_t get_count = 0;
        size_t put_count = 0;

        KeyValue kv;
        while(_ins[n]->_pipe.get(kv)) {
            ++get_count = 0;
            std::string line = kv._key;
            if(!kv._value.empty())
                line += '\t' + kv._value;
            (*_outs)[n]->_pipe.put(line);
            ++put_count;
        }
        update_metrics(n, get_count, put_count);
    }
};

class BSink : public Block<std::string, std::string>
{
public:
    BSink(const std::string tag, size_t channels) : Self(tag, channels)
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
        size_t get_count = 0;
        size_t put_count = 0;

        std::string line;
        while(_ins[n]->_pipe.get(line)) {
            ++get_count;
            (*_outs)[0]->_pipe.put(line);
            ++put_count;
        }
        update_metrics(n, get_count, put_count);
    }

};

class BWriteFile : public BlockLast<std::string>
{
public:
    std::ostream& _out;
    BWriteFile(const std::string tag, std::ostream& out) : Self(tag, 1), _out(out)
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
        size_t get_count = 0;
        std::string line;
        while(_ins[n]->_pipe.get(line)) {
            ++get_count;
            _out << line << "\n";
        }
        if(!_tag.empty()) {
            metrics_t m;
            m[_tag + ".get_count"] = get_count;
            std::string prefix = _tag + "." + std::to_string(n);
            m[prefix + ".get_count"] = get_count;
            Setup::get()._metrics.update(m);
        }
    }
};
