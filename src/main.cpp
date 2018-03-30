#include "../bin/version.h"

#include <string>
#include <iostream>
#include <chrono>

#include "block.h"
#include "shortest_uniq.h"

int main(int argc, char** argv)
{
    if(argc != 4) {
        std::cout << "Usage " << argv[0] << " src M R" << std::endl;
        return 1;
    }

    std::string src  = argv[1];
    size_t M = std::atol(argv[2]);
    size_t R = std::atol(argv[3]);

    // allocate blocks
    BSplitFile split_file("s01.split_file");
    BReadFile read_file("s02.read_file",M);
    BConvertToKV convert_to_kv("s03.convert_to_kv",M);

    shortest_uniq::BP1m bp1m("s04.bp1m", M);

    BShard shard1("s05.shard", M);
    BSort sort1("s06.sort", R);
    shortest_uniq::BP2r bp2r("s07.bp2r",R);

    BShard shard2("s08.shard",R);
    BSort sort2("s09.sort",R);
    shortest_uniq::BP3r bp3r("s10.bp3r", R);

    BConvertFromKV convert_from_kv("s11.convert_from_kv",R);
    BSink sink("s12.sink",R);
    BWriteFile write_file("s13.write_file", std::cout);

    // attach blocks
    split_file.attach(read_file._ins);
    read_file.attach(convert_to_kv._ins);
    convert_to_kv.attach(bp1m._ins);

    bp1m.attach(shard1._ins);

    shard1.attach(sort1._ins);
    sort1.attach(bp2r._ins);
    bp2r.attach(shard2._ins);

    shard2.attach(sort2._ins);
    sort2.attach(bp3r._ins);
    bp3r.attach(convert_from_kv._ins);

    convert_from_kv.attach(sink._ins);
    sink.attach(write_file._ins);

    // run blocks
    split_file.run();
    read_file.run();
    convert_to_kv.run();

    bp1m.run();

    shard1.run();
    sort1.run();
    bp2r.run();

    shard2.run();
    sort2.run();
    bp3r.run();

    convert_from_kv.run();
    sink.run();
    write_file.run();

    // initiate procesing
    split_file._ins[0]->_pipe.put(src);

    // wait for blocks to finish processing
    split_file.done();
    read_file.done();
    convert_to_kv.done();

    bp1m.done();

    shard1.done();
    sort1.done();
    bp2r.done();

    shard2.done();
    sort2.done();
    bp3r.done();

    convert_from_kv.done();
    sink.done();

    write_file.finish();
    write_file.join();

    return 0;

}
