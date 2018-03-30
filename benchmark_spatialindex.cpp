//
// Copyright (C) 2013 Mateusz Loskot <mateusz@loskot.net>
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy
// at http://www.boost.org/LICENSE_1_0.txt)
//
#include "spatial_index_benchmark.hpp"
#include <spatialindex/SpatialIndex.h>

#include "mem_stats.hpp"


using namespace std;
namespace si = SpatialIndex;

namespace {

inline std::ostream& print_result(std::ostream& os, std::string const& /*lib*/, sibench::result_info const& load, sibench::result_info const& query, si::ISpatialIndex const &i)
{
    assert(load.min_capacity == load.min_capacity);
    assert(query.max_capacity == query.max_capacity);
    si::IStatistics* pstat = nullptr;
    i.getStatistics(&pstat);
    std::unique_ptr<si::IStatistics> stat(pstat);
    std::string mem_size =  mem_stats::get_mem();
    std::streamsize wn(5), wf(18);
    os << std::left << std::setfill(' ') << std::fixed << std::setprecision(6)
       << std::setw(wn) << load.max_capacity
       << std::setw(wn) << load.min_capacity
       << std::setw(wf) << load.min
       << std::setw(wf) << query.min
       << std::setw(wf) << stat->getReads()
       << std::setw(wf) << stat->getWrites()
       << std::setw(wf) << stat->getNumberOfNodes()
       << std::setw(wf) << stat->getNumberOfData()
       << std::setw(wf) << mem_stats::get_filesize("/tmp/kalamashaka.idx")
       << std::setw(wf) << mem_stats::get_filesize("/tmp/kalamashaka.dat")
       << std::setw(wf) << mem_size
       << std::endl;
    return os;
}

inline std::ostream& print_result_header(std::ostream& os, std::string const& lib)
{
    std::streamsize const wn(5), wf(18), vn(2);
    os << sibench::get_banner(lib) << ' ' << std::setw(wn * vn + wf * vn) << std::setfill('-') << ' ' << std::endl;
    os << std::left << std::setfill(' ')
       << std::setw(wn * vn) << "capacity" << std::setw(wf) << "load" << std::setw(wf) << "query"
       << std::setw(wf) << "reads" << std::setw(wf) << "writes" << std::setw(wf) << "nodes" << std::setw(wf) << "ndata"
       << std::setw(wf) << "i_file" << std::setw(wf) << "d_file"
       << std::setw(wf) << "memory"
       << std::endl;
    return os;
}

void print_statistics(std::ostream& os, std::string const& lib, si::ISpatialIndex const& i)
{
    si::IStatistics* pstat = nullptr;
    i.getStatistics(&pstat);
    std::unique_ptr<si::IStatistics> stat(pstat);
    os << sibench::get_banner(lib)
       << " stats: reads=" << stat->getReads()
       << ", writes=" << stat->getWrites()
       << ", nodes=" << stat->getNumberOfNodes()
       << ", ndata=" << stat->getNumberOfData()
     //  << ", splits=" << i.m_u64Hits.m_u64Splits
      // << ", hits=" << stat->getHits()
      // << ", misses=" << stat->getMisses()
       //<< ", adjustments=" << stat->getAdjustments()
      // << ", height=" << stat->getTreeHeight()
       << std::endl;
}

si::RTree::RTreeVariant get_variant()
{
    auto const rv = sibench::get_rtree_split_variant().first;
    switch(rv)
    {
    case sibench::rtree_variant::linear:
        return si::RTree::RV_LINEAR;
    case sibench::rtree_variant::quadratic:
        return si::RTree::RV_QUADRATIC;
    case sibench::rtree_variant::rstar:
        return si::RTree::RV_RSTAR;
    default:
        throw std::runtime_error("unknown rtree variant");
    };
}

struct query_visitor : public si::IVisitor
{
   query_visitor() : m_io_index(0), m_io_leaf(0), m_io_found(0) {}

    void visitNode(si::INode const& n)
    {
        n.isLeaf() ? ++m_io_leaf : ++m_io_index;
    }

    void visitData(si::IData const& d)
    {
        si::IShape* ps = nullptr;
        d.getShape(&ps);
        std::unique_ptr<si::IShape> shape(ps);
        ; // use shape

        // Region is represented as array of characters
        uint8_t* pd = 0;
        uint32_t size = 0;
        d.getData(size, &pd);
        //std::unique_ptr<uint8_t[]> data(pd);
        // use data
        //std::string str(reinterpret_cast<char*>(pd));

        //cout << d.getIdentifier() << endl; // ID is query answer
        ++m_io_found;
    }

    void visitData(std::vector<si::IData const*>& v)
    {
        // TODO
        assert(!v.empty()); (void)v;
        //cout << v[0]->getIdentifier() << " " << v[1]->getIdentifier() << endl;
    }

    size_t m_io_index;
    size_t m_io_leaf;
    size_t m_io_found;
};

#ifdef SIBENCH_RTREE_LOAD_BLK
struct data_stream : public si::IDataStream
{
    data_stream(sibench::boxes2d_t const& boxes)
        : boxes(boxes), next(0), pdnext(nullptr)
    {
        get_next();
    }
    ~data_stream()
    {
        delete pdnext;
    }

    si::IData* getNext()
    {
        if (!pdnext) return 0;
        si::RTree::Data* pcurrent = pdnext;
        get_next();
        return pcurrent;
    }

    bool hasNext()
    {
        return pdnext != nullptr;
    }

    uint32_t size()
    {
        return boxes.size();
    }

    void rewind()
    {
        if (pdnext)
        {
            delete pdnext;
            pdnext = nullptr;
        }
        next = 0;
    }

    void get_next()
    {
        pdnext = nullptr;
        if (next < boxes.size())
        {
            auto const& box = boxes[next];
            double low[2] = { std::get<0>(box), std::get<1>(box) };
            double high[2] =  { std::get<2>(box), std::get<3>(box) };
            si::Region region(low, high, 2);
            si::id_type id(next);
            pdnext = new si::RTree::Data(sizeof(double), reinterpret_cast<byte*>(low), region, id);
            ++next;
        }

    }

    sibench::boxes2d_t const& boxes;
    sibench::boxes2d_t::size_type next;
    si::RTree::Data* pdnext;

private:
    data_stream(data_stream const&); /*=delete*/
    data_stream& operator=(data_stream const&); /*=delete*/
};
#endif // SIBENCH_RTREE_LOAD_BULK

} // unnamed namespace


void run_benchmark(std::string const& filename,std::string const& query_file,  int dim, int next_capacity) {

    std::string const lib("lsi");
    double const fill_factor = 0.5; // default: 0.7 // TODO: does it mean index_capacity * fill_factor?
    auto const boxes = sibench::generate_mm_2d(filename, dim);
    // accumulated results store (load, query)
    sibench::result_info load_r;
    sibench::result_info query_r;

    // index parameters
    std::size_t const min_capacity = next_capacity;
    std::size_t const max_capacity = std::size_t(std::floor(min_capacity / fill_factor));

    load_r.min_capacity = query_r.min_capacity = min_capacity;
    load_r.max_capacity = query_r.max_capacity = max_capacity;

    typedef std::array<double, 2> coord_array_t;
    si::RTree::RTreeVariant const variant = get_variant();
    uint32_t const index_capacity = max_capacity; // default: 100
    uint32_t const leaf_capacity = max_capacity; // default: 100
    uint32_t const dimension = 2;
    si::id_type index_id;
    std::string base_name = "/tmp/kalamashaka";
    // std::unique_ptr<si::IStorageManager> sm(si::StorageManager::createNewMemoryStorageManager())
    std::unique_ptr<si::IStorageManager> sm(si::StorageManager::createNewDiskStorageManager(base_name, 4096));;

#ifdef SIBENCH_RTREE_LOAD_ITR
    std::unique_ptr<si::ISpatialIndex> rtree(si::RTree::createNewRTree(*sm,
        fill_factor, index_capacity, leaf_capacity, dimension, variant, index_id));

    // Benchmark: insert
    {
        auto const marks = sibench::benchmark("insert", boxes.size(), boxes,
            [&rtree] (sibench::boxes2d_t const& boxes, std::size_t iterations)
        {
            auto const s = iterations < boxes.size() ? iterations : boxes.size();
            for (size_t i = 0; i < s; ++i)
            {
                auto const& box = boxes[i];
                coord_array_t const p1 = { std::get<0>(box), std::get<1>(box) };
                coord_array_t const p2 = { std::get<2>(box), std::get<3>(box) };
                si::Region region(
                    si::Point(p1.data(), p1.size()),
                    si::Point(p2.data(), p2.size()));
                si::id_type item_id(i);
                rtree->insertData(0, nullptr, region, item_id);
            }
        });

        load_r.accumulate(marks);
        // debugging
        //sibench::print_result(std::cout, lib, marks);
        //print_statistics(std::cout, lib, *rtree);
    }
#elif SIBENCH_RTREE_LOAD_BLK
    std::unique_ptr<si::ISpatialIndex> rtree;
    // Benchmark: bulk loading (Split-Tile-Recurse)
    {
        si::IStorageManager* psm = sm.get();
        auto const marks = sibench::benchmark("insert", boxes.size(), boxes,
            [&rtree, &psm, fill_factor, index_capacity, leaf_capacity, dimension, variant, &index_id] (sibench::boxes2d_t const& boxes, std::size_t /*iterations*/)
        {
            data_stream dstream(boxes);
            std::unique_ptr<si::ISpatialIndex> rtree_tmp(si::RTree::createAndBulkLoadNewRTree(si::RTree::BLM_STR,
                dstream, *psm, fill_factor, index_capacity, leaf_capacity, dimension, variant, index_id));
            rtree = std::move(rtree_tmp);
        });

        load_r.accumulate(marks);
        // debugging
        //sibench::print_result(std::cout, lib, marks);
        //print_statistics(std::cout, lib, *rtree);
    }
#else
#error Unknown rtree loading method
#endif

    // Benchmark: query
    {
        size_t query_found = 0;

        auto const marks = sibench::benchmark("query", sibench::max_queries, boxes,
            [&rtree, &query_found, dim, query_file] (sibench::boxes2d_t const& boxes, std::size_t iterations)
        {
            auto const query_boxes = sibench::generate_mm_2d(query_file, dim);

            for (size_t i = 0; i < query_boxes.size(); ++i)
            {
                //std::cout << "Qeurying" << std::endl;
                auto const& box = query_boxes[i];
                coord_array_t const p1 = { std::get<0>(box), std::get<1>(box) };
                coord_array_t const p2 = { std::get<2>(box), std::get<3>(box) };
                si::Region region(
                    si::Point(p1.data(), p1.size()),
                    si::Point(p2.data(), p2.size()));
                query_visitor qvisitor;
                rtree->intersectsWithQuery(region, qvisitor);

                query_found += qvisitor.m_io_found;
                //std::cout << qvisitor.m_io_found << " Query found" << std::endl;
            }
            /*for (size_t i = 0; i < iterations; ++i)
            {
                auto const& box = boxes[i];
                coord_array_t const p1 = { std::get<0>(box) - 10, std::get<1>(box) - 10 };
                coord_array_t const p2 = { std::get<2>(box) + 10, std::get<3>(box) + 10 };
                si::Region region(
                    si::Point(p1.data(), p1.size()),
                    si::Point(p2.data(), p2.size()));
                query_visitor qvisitor;
                rtree->intersectsWithQuery(region, qvisitor);

                query_found += qvisitor.m_io_found;
            }*/
        });

        query_r.accumulate(marks);

        // debugging
        //sibench::print_result(std::cout, lib, marks);
        //sibench::print_query_count(std::cout, lib, query_found);
    }

    // single line per run
    sm->flush();
    print_result(std::cout, lib, load_r, query_r, *rtree);
}

int main(int argc, char* argv[])
{
    try
    {
        std::string const lib("lsi");
        int node_size = std::atoi(argv[4]);
        int dim = std::atoi(argv[3]);
        if (node_size == 2) {
            print_result_header(std::cout, lib);
        }

        run_benchmark(argv[1], argv[2], dim, node_size);

        return EXIT_SUCCESS;
    }
    catch (Tools::Exception& e)
    {
        std::cerr << e.what() << std::endl;
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
