
#include "mm_rstar.hpp"



/**
 * Reads the query and data files in the mathematik repository
 *
 * @param filename. The file to load
 * @param dim. The number of dimensions contained in the data. This
 * determines the size of the bounding box and the rectangles to be generated
 *
 * @return T*. This is the type that the data will be stored in.
 *
 */
int main() {
    int dim =2;
    auto const boxes = mmrstar::read_file("rtree_data/data/rea03", dim);
    for (auto const& data: boxes) {

        std::cout << data.dimen[0].l << " " << data.dimen[0].h << std::endl;
        std::cout << data.dimen[1].l << " " << data.dimen[1].h << std::endl;
        std::cout << data.dimen[2].l << " " << data.dimen[2].h << std::endl;
        std::cout << std::endl;
    }
    return 0;
}
