#include "btree/BTree.hpp"
#include "util/Utility.hpp"
#include "common/Config.hpp"
#include "buffer_manager/BufferManager.hpp"
#include "segment_manager/SegmentManager.hpp"
#include "segment_manager/BTreeSegment.hpp"
#include <gtest/gtest.h>
#include <string>
#include <cstdint>
#include <cassert>
#include <vector>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <thread>


const int CHARSIZE = 20;

/* Comparator functor for uint64_t*/
struct MyCustomUInt64Cmp {
   bool operator()(uint64_t a, uint64_t b) const {
      return a<b;
   }
};

template <unsigned len>
struct Char {
   char data[len];
};

/* Comparator functor for char */
template <unsigned len>
struct MyCustomCharCmp {
   bool operator()(const Char<len>& a, const Char<len>& b) const {
      return memcmp(a.data, b.data, len) < 0;
   }
};

typedef std::pair<uint32_t, uint32_t> IntPair;

/* Comparator for IntPair */
struct MyCustomIntPairCmp {
   bool operator()(const IntPair& a, const IntPair& b) const {
      if (a.first < b.first)
         return true;
      else
         return (a.first == b.first) && (a.second < b.second);
   }
};

template <class T>
const T& getKey(const uint64_t& i);

template <>
const uint64_t& getKey(const uint64_t& i) { return i; }

std::vector<std::string> char20;
template <>
const Char<CHARSIZE>& getKey(const uint64_t& i) {
   std::stringstream ss;
   ss << i;
   std::string s(ss.str());
   char20.push_back(std::string(CHARSIZE-s.size(), '0')+s);
   return *reinterpret_cast<const Char<CHARSIZE>*>(char20.back().data());
}

std::vector<IntPair> intPairs;
template <>
const IntPair& getKey(const uint64_t& i) {
   intPairs.push_back(std::make_pair(i/3, 3-(i%3)));
   return intPairs.back();
}


template <class T, class CMP>
void test(uint64_t n) {
    typedef dbi::TID TID;
    const std::string fileName = "swap_file";
    const uint32_t pages = 10000;

    // Create
    ASSERT_TRUE(dbi::util::createFile(fileName, pages * dbi::kPageSize));
    dbi::BufferManager bufferManager(fileName, pages / 2);
    dbi::SegmentManager segmentManager(bufferManager, true);
    dbi::SegmentId id = segmentManager.createSegment(dbi::SegmentType::BT, 10);
    dbi::BTreeSegment& segment = segmentManager.getBTreeSegment(id);

    // Set up stuff, you probably have to change something here to match to your interfaces
    dbi::BTree<T, CMP> bTree(segment);
    //std::cout << "LeafnodeSize: " << bTree.getLeafNodeSize() << std::endl;
    //std::cout << "Starting test\n";
    // Insert values
    TID tid;
    for (uint64_t i=0; i<n; ++i) {
        //std::cout << i << std::endl;
        ASSERT_TRUE(bTree.insert(getKey<T>(i),static_cast<TID>(i*i)));
        ASSERT_TRUE(bTree.lookup(getKey<T>(i),tid));
    }
    ASSERT_EQ(bTree.size(), n);
    // Check if they can be retrieved
    for (uint64_t i=0; i<n; ++i) {
        TID tid;
        ASSERT_TRUE(bTree.lookup(getKey<T>(i),tid));
        ASSERT_EQ(tid, i*i);
    }
   
    // Delete some values
    for (uint64_t i=0; i<n; ++i)
        if ((i%7)==0)
            ASSERT_TRUE(bTree.erase(getKey<T>(i)));
    // Check if the right ones have been deleted
    for (uint64_t i=0; i<n; ++i) {
        TID tid;
        if ((i%7)==0) {
            ASSERT_FALSE(bTree.lookup(getKey<T>(i),tid));
        } else {
            ASSERT_TRUE(bTree.lookup(getKey<T>(i),tid));
            ASSERT_EQ(tid, i*i);
        }
    }
    // Delete everything
    for (uint64_t i=0; i<n; ++i)
        bTree.erase(getKey<T>(i));
    ASSERT_EQ(bTree.size(), (uint64_t)0);

    remove(fileName.c_str());
}

//const uint64_t n = 10*1000ul;

TEST(BTreeTest, FunkeTestUintKey) {
    uint64_t n = 1000*1000ul;
    // Test index with 64bit unsigned integers
    test<uint64_t, MyCustomUInt64Cmp>(n);
}

TEST(BTreeTest, FunkeTestCharKey) {
    uint64_t n = 10*1000ul;
    // Test index with CHARSIZE character strings
    test<Char<CHARSIZE>, MyCustomCharCmp<CHARSIZE>>(n);
}

TEST(BTreeTest, FunkeTestCompoundKey) {
    uint64_t n = 1000*1000ul;
    // Test index with compound key
    test<IntPair, MyCustomIntPairCmp>(n);

}

TEST(BTreeTest, SimpleTest) {
    typedef dbi::TID TID;
    const std::string fileName = "swap_file";
    const uint32_t pages = 100;

    // Create
    ASSERT_TRUE(dbi::util::createFile(fileName, pages * dbi::kPageSize));
    dbi::BufferManager bufferManager(fileName, pages / 2);
    dbi::SegmentManager segmentManager(bufferManager, true);
    dbi::SegmentId id = segmentManager.createSegment(dbi::SegmentType::BT, 10);
    dbi::BTreeSegment& segment = segmentManager.getBTreeSegment(id);

    dbi::BTree<uint64_t, std::less<uint64_t>> tree(segment);
    TID tid; 
    
    EXPECT_FALSE(tree.lookup((uint64_t)2, tid));
    
    for (uint64_t i=1; i <= tree.getLeafNodeSize()+2; i++) {
        EXPECT_TRUE(tree.insert(i, i));
        EXPECT_TRUE(tree.lookup(i, tid));
    }

    // Test Iterator 
    auto it = tree.lookupRange(1, tree.getLeafNodeSize()+2);
    for (uint64_t i=1; i <= tree.getLeafNodeSize()+2; i++) {
        EXPECT_TRUE(it.valid());
        std::pair<uint64_t, TID> p = *it;
        EXPECT_EQ(p.first, i); 
        it++;
    }

    EXPECT_FALSE(it.valid());
    tree.visualize();

    remove(fileName.c_str());
}

void threadTestInsert(dbi::BTree<uint64_t>* tree, uint64_t n, uint64_t numthreads, uint64_t threadid) {
    dbi::TID tid;
    // Insert values into tree
    for (uint64_t i=threadid; i <= n; i+=numthreads) {
        ASSERT_TRUE(tree->insert(i, i));
        ASSERT_TRUE(tree->lookup(i, tid));
        ASSERT_EQ(i, tid);
    }
}

void threadTestErase(dbi::BTree<uint64_t>* tree, uint64_t n, uint64_t numthreads, uint64_t threadid) {
    dbi::TID tid;
    // Delete values
    for (uint64_t i=(threadid+1)%numthreads; i <= n; i+=numthreads) {
        ASSERT_TRUE(tree->erase(i));
        ASSERT_FALSE(tree->lookup(i, tid));
    }
}


void threadTestLookup(dbi::BTree<uint64_t>* tree, uint64_t n, volatile bool* finish) {
    dbi::TID tid;
    srand(n);
    while (!*finish) {
        uint64_t i = rand() % n;
        tree->lookup(i, tid);
    }
}


TEST(BTreeTest, ThreadTest) {
    const std::string fileName = "swap_file";
    const uint32_t pages = 10000;
    const uint64_t n = 1000*1000ul;
    const uint64_t numthreads = 4;
    volatile bool finish = false; 
    // Create
    ASSERT_TRUE(dbi::util::createFile(fileName, pages * dbi::kPageSize));
    dbi::BufferManager bufferManager(fileName, pages / 4);
    dbi::SegmentManager segmentManager(bufferManager, true);
    dbi::SegmentId id = segmentManager.createSegment(dbi::SegmentType::BT, 10);
    dbi::BTreeSegment& segment = segmentManager.getBTreeSegment(id);
    dbi::BTree<uint64_t> tree(segment);
    
    std::array<std::thread, numthreads> threads;
    
    std::thread tlookup(threadTestLookup, &tree, n, &finish);

    // Create and start threads to do concurrent insert
    for (uint64_t i=0; i < numthreads; i++) {
        threads[i] = std::thread(threadTestInsert, &tree, n, numthreads, i);
    }
    
    // Wait for threads to finish
    for (uint64_t i=0; i < numthreads; i++) {
        threads[i].join();
    }
    
    // Check values are all inserted
    dbi::TID tid;
    for (uint64_t i=0; i <= n; i++) {
        ASSERT_TRUE(tree.lookup(i, tid));
        ASSERT_EQ(i, tid);
    }
    
    // Create and start threads to do concurrent erase 
    for (uint64_t i=0; i < numthreads; i++) {
        threads[i] = std::thread(threadTestErase, &tree, n, numthreads, i);
    }
    
    // Wait for threads to finish
    for (uint64_t i=0; i < numthreads; i++) {
        threads[i].join();
    }
    finish = true;
    tlookup.join();  
    // Make sure tree is empty 
    ASSERT_EQ(tree.size(), (uint64_t)0);

    remove(fileName.c_str());
}

