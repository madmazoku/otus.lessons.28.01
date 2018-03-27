#pragma once

#include <vector>
#include <fstream>

#include "map_reduce.h"

struct FileRange {
    std::string _file_name;
    size_t _start;
    size_t _end;

    FileRange(const std::string& file_name, size_t start, size_t end) : _file_name(file_name), _start(start), _end(end) {}
    FileRange(const FileRange& fr) : _file_name(fr._file_name), _start(fr._start), _end(fr._end) {}
};

using FileRanges = std::vector<FileRange>;

FileRanges split_file(const std::string& file_name, size_t N) {
    std::ifstream in(file_name, std::ifstream::ate | std::ifstream::binary);
    size_t file_size = in.tellg(); 

    FileRanges frs;
    size_t n = N + 1;
    size_t start = 0;
    while(start < file_size) {
        size_t length = (file_size - start) / --n;
        if(length == 0)
            length = file_size - start;

        FileRange fr(file_name, start, start + length);

        in.seekg(fr._end);
        while(!in.eof()) {
            ++fr._end;
            if(in.get() == '\n')
                break;
        }
        frs.push_back(fr);
        start = fr._end ;
    }

    return frs;
}
