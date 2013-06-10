//
// Copyright (C) 2013 Mateusz Loskot <mateusz@loskot.net>
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy 
// at http://www.boost.org/LICENSE_1_0.txt)
//
#include "spatial_index_benchmark.hpp"
#ifdef SIBENCH_RTREE_LOAD_STR
#define BOOST_GEOMETRY_INDEX_DETAIL_EXPERIMENTAL 1
#endif
#include <boost/geometry/index/rtree.hpp>
// enable internal debugging utilities
#include <boost/geometry/index/detail/rtree/utilities/statistics.hpp>
#include <boost/geometry/index/detail/rtree/utilities/print.hpp>
#include <boost/tuple/tuple.hpp>
using namespace std;
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

template <typename T>
void print_statistics(std::ostream& os, std::string const& lib, T const& i)
{
    using bgi::detail::rtree::utilities::statistics;
    auto const s = statistics(i);
    os << sibench::get_banner(lib)
       << " stats: levels=" << boost::get<0>(s)
       << ", nodes=" << boost::get<1>(s)
       << ", leaves=" << boost::get<2>(s)
       << ", values=" << boost::get<3>(s)
       << ", values_min=" << boost::get<4>(s)
       << ", values_max=" << boost::get<5>(s)
       << std::endl;
}

int main()
{
    try
    {
#ifdef SIBENCH_BGI_RTREE_PARAMS_CT
        std::string const lib("bgi_ct");
#elif SIBENCH_BGI_RTREE_PARAMS_RT
        std::string const lib("bgi_rt");
#else
#error Unknown Boost.Geometry rtree parameters variant
#endif
        
        // Generate random objects for indexing
        auto boxes = sibench::generate_boxes(sibench::max_insertions);

        // Set up index
        std::size_t const max_capacity = 100;
        std::size_t const min_capacity =  50; // default: max * 0.3

        typedef bg::model::point<double, 2, bg::cs::cartesian> point_t;
        typedef bg::model::box<point_t> box_t;
#ifdef SIBENCH_BGI_RTREE_PARAMS_CT
    #ifdef SIBENCH_RTREE_SPLIT_LINEAR
        typedef bgi::rtree<box_t, bgi::linear<max_capacity, min_capacity>> rtree_t;
    #elif SIBENCH_RTREE_SPLIT_QUADRATIC
        typedef bgi::rtree<box_t, bgi::quadratic<max_capacity, min_capacity>> rtree_t;
    #else
        typedef bgi::rtree<box_t, bgi::rstar<max_capacity, min_capacity>> rtree_t;
    #endif    
        rtree_t rtree;
#elif SIBENCH_BGI_RTREE_PARAMS_RT
    #ifdef SIBENCH_RTREE_SPLIT_LINEAR
        typedef bgi::dynamic_linear rtree_parameters_t;
    #elif SIBENCH_RTREE_SPLIT_QUADRATIC
        typedef bgi::dynamic_quadratic rtree_parameters_t;
    #else
        typedef bgi::dynamic_rstar rtree_parameters_t;
    #endif
        typedef bgi::rtree<box_t, rtree_parameters_t> rtree_t;
        rtree_t rtree(rtree_parameters_t(max_capacity, min_capacity));
#endif

#if 0
#ifdef SIBENCH_RTREE_LOAD_STR
        // No SRT loading available, just check if pre-sorting affects loading times
        std::sort(boxes.begin(), boxes.end(), [](sibench::box2d_t const& b1, sibench::box2d_t const& b2)
        {
             return std::get<2>(b1) + std::get<0>(b1) < std::get<2>(b2) + std::get<0>(b2);
        });
#endif
#endif

#ifdef SIBENCH_RTREE_LOAD_STR
        typedef std::vector<box_t> input_boxes_t;
        input_boxes_t bgboxes;
        bgboxes.reserve(boxes.size());
        for(auto const& box : boxes)
        {
            point_t p1(std::get<0>(box), std::get<1>(box));
            point_t p2(std::get<2>(box), std::get<3>(box));
            bgboxes.emplace_back(p1, p2);
        }
        auto const& input_boxes = bgboxes;
#else
        typedef sibench::boxes2d_t input_boxes_t;
        auto const& input_boxes = boxes;
#endif // SIBENCH_RTREE_LOAD_STR

        // Benchmark: insert
        {
            auto const marks = sibench::benchmark("insert", input_boxes.size(), input_boxes,
                [&rtree] (input_boxes_t const& boxes, std::size_t iterations)
            {
#ifdef SIBENCH_RTREE_LOAD_STR
                boost::ignore_unused_variable_warning(iterations);

                rtree.insert(boxes.cbegin(), boxes.cend());
#else
                auto const s = iterations < boxes.size() ? iterations : boxes.size();
                for (size_t i = 0; i < s; ++i)
                {
                    auto const& box = boxes[i];
                    point_t p1(std::get<0>(box), std::get<1>(box));
                    point_t p2(std::get<2>(box), std::get<3>(box));
                    box_t region(p1, p2);
                    rtree.insert(region);
                }
#endif
            });
            sibench::print_result(std::cout, lib, marks);
        
            //std::cout <<  rtree;
            print_statistics(std::cout, lib, rtree);
        }

        // Benchmark: query
        {
            size_t query_found = 0;

            auto const marks = sibench::benchmark("query", sibench::max_queries, boxes,
                [&rtree, &query_found] (sibench::boxes2d_t const& boxes, std::size_t iterations)
            {
                std::vector<box_t> result;
                result.reserve(iterations);
                
                for (size_t i = 0; i < iterations; ++i)
                {
                    result.clear();
                    auto const& box = boxes[i];
                    point_t p1(std::get<0>(box) - 10, std::get<1>(box) - 10);
                    point_t p2(std::get<2>(box) + 10, std::get<3>(box) + 10);
                    box_t region(p1, p2);
                    rtree.query(bgi::intersects(region), std::back_inserter(result));
                    query_found += result.size();
                }
            });
            sibench::print_result(std::cout, lib, marks);
            sibench::print_query_count(std::cout, lib, query_found);
        }

        return EXIT_SUCCESS;
    }
    catch (std::exception const& e)
    {
        std::cerr << e.what() << std::endl;
    }
    catch (...)
    {
        std::cerr << "unknown error" << std::endl;
    }
    return EXIT_FAILURE;
}
