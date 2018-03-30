#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>

#include <boost/algorithm/string.hpp>

namespace mem_stats {

    std::string get_mem() {
        std::string line;
        std::string mem_val;
        std::string prefix = "VmRSS:";
        std::ifstream input("/proc/self/status");
        while (std::getline(input, line)) {
            if (line.substr(0, prefix.size()) == prefix) {
                mem_val = line.substr(prefix.size(), line.size());
                boost::trim(mem_val);
            }
        }
        return mem_val;
    }

    std::fstream::pos_type get_filesize(std::string filename) {
        std::fstream file;
        std::ios_base::iostate exceptionMask = file.exceptions() | std::ios::failbit;
        file.exceptions(exceptionMask);
        try {
            file.open(filename);
            file.seekg(0, std::ifstream::end);
        } catch(std::ios_base::failure& e) {
            std::cerr << e.what() << std::endl;
        }
        return file.tellg();
    }
}
