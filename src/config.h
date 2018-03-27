#pragma once

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

class Config {
public:
    std::string _src;
    std::string _dst;
    std::string _tmp;

    boost::property_tree::ptree _mapreduce;
    std::string _type;

    size_t _mappers;
    size_t _reducers;

    Config(const char* file_name) {
        boost::property_tree::ptree root;
        boost::property_tree::read_json(file_name, root);

        _src = root.get<std::string>("src");
        _dst = root.get<std::string>("dst");
        _tmp = root.get<std::string>("tmp", "/tmp/");

        _mapreduce = root.get_child("mapreduce");

        _mappers = root.get<size_t>("mappers", 10);
        _reducers = root.get<size_t>("reducers", 10);
    }
};