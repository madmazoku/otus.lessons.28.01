#pragma once

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

class Config {
private:

public:
    std::string _src;
    std::string _dst;
    std::string _tmp;

    std::vector<boost::property_tree::ptree> _stages;
    std::string _type;

    size_t _mappers;
    size_t _reducers;
    size_t _pipe_size;

    Config(const char* file_name) {
        boost::property_tree::ptree root;
        boost::property_tree::read_json(file_name, root);

        _src = root.get<std::string>("src");
        _dst = root.get<std::string>("dst");
        _tmp = root.get<std::string>("tmp", "/tmp/");

        for (auto stage : root.get_child("stages"))
            _stages.push_back(stage.second);

        _mappers = root.get<size_t>("mappers", 10);
        _reducers = root.get<size_t>("reducers", 10);
        _pipe_size = root.get<size_t>("pipe_size", 10);
    }
};