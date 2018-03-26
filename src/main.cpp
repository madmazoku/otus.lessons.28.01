
#include <iostream>
#include "../bin/version.h"

#include "script.h"

#include <tuple>
#include <algorithm>


int main(int argc, char** argv)
{
	Script script;
	script.init();
	script.load(argv[1]);

	std::vector<std::tuple<std::string, std::string>> key_value;

	std::string line;
	while(std::getline(std::cin, line)) {
		std::string key, value;
		script.parse(line, key, value);
		key_value.push_back(std::make_tuple(key, value));
		std::cout << "c++ in   > key: " << key << "; value: " << value << std::endl;
	}

	std::sort(key_value.begin(), key_value.end(), [](auto a, auto b) { return std::get<0>(a) < std::get<0>(b); });

	std::vector<std::tuple<std::string, std::vector<std::string>>> key_values;
	auto it_kv_curr = key_value.end();
	for(auto it_kv = key_value.begin(); it_kv != key_value.end(); ++it_kv) {
		std::cout << "c++ part > key: " << std::get<0>(*it_kv) << "; value: " << std::get<1>(*it_kv) << std::endl;
		if(it_kv_curr == key_value.end() || std::get<0>(*it_kv_curr) != std::get<0>(*it_kv)) {
			key_values.push_back(std::make_tuple(std::get<0>(*it_kv), std::vector<std::string>{std::get<1>(*it_kv)}));
			it_kv_curr = it_kv;
		} else {
			std::get<1>(key_values.back()).push_back(std::get<1>(*it_kv));
		}
	}

	for(auto kvs : key_values) {
		std::cout << "c++ out  > key: " << std::get<0>(kvs) << std::endl;
		for(auto v : std::get<1>(kvs)) {
			std::cout << "\t\t" << v << std::endl;
			std::string line;
		}
		std::vector<std::string> lines;
		script.collect(lines, std::get<0>(kvs), std::get<1>(kvs));
		for(auto l : lines) {
			std::cout << l << std::endl;
		}
	}

	script.done();

    return 0;
}
