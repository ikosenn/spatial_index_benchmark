#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <utility>
#include <memory>
#include <iostream>


typedef struct {
    double l, h;
} typinterval;

typedef struct {
    std::vector<typinterval> dimen; // contains data for a single dimension
} dataStore;

namespace mmrstar {

    /**
     * Reads the query and data files in the mathematik repository
     *
     * @param filename. The file to load
     * @param dim. The number of dimensions contained in the data. This
     * determines the size of the bounding box and the rectangles to be generated
     *
     * @return: A vector that contains the dimensions for all the records .
     *
     */
    inline std::vector<dataStore> read_file(std::string const& filename, int& dim) {
        int datafile;
        int ferr, length, num_rects, nbytes, i, j;
        struct stat status;
        typedef typinterval typrect[dim];
        std::vector<dataStore> data;

        typrect rectangle;

        datafile = open(filename.c_str(), O_RDONLY);
        ferr = fstat(datafile, &status);
        length = status.st_size;
        num_rects = length / sizeof(typrect);

        //std::cout << "Number of rectangles: " << num_rects << std::endl;
        //std::cout << "Size of a rectangle: " << sizeof(typrect) << std::endl;

        i = 0;
        do {
            nbytes = read(datafile, rectangle, sizeof(typrect));
            if (nbytes != sizeof(typrect)) {
                std::cout << "Error while readig file" << std::endl;
                exit(1);
            }

            dataStore temp_store;
            for (int j = 0; j < dim; j++) {
                temp_store.dimen.push_back(rectangle[j]);
            }
            data.push_back(temp_store);

            i += 1;
        } while ( i < num_rects);
        close(datafile);

        return data;
    }
} // end namespace mmrstar


