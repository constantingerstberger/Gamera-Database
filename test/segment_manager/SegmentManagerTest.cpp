#include "FunkeSlottedTest.hpp"
#include "util/Utility.hpp"
#include "common/Config.hpp"
#include "buffer_manager/BufferManager.hpp"
#include "test/TestConfig.hpp"
#include "segment_manager/SegmentManager.hpp"
#include "segment_manager/SPSegment.hpp"
#include "gtest/gtest.h"
#include "segment_manager/Record.hpp"
#include "operator/TableScanOperator.hpp"
#include <array>
#include <fstream>
#include <string>
#include <set>

using namespace std;
using namespace dbi;

TEST(SegmentManager, Simple)
{
   const uint32_t pages = 100;
   assert(kSwapFilePages>=pages);

   BufferManager bufferManager(kSwapFileName, pages / 2);
   SegmentManager segmentManager(bufferManager, true);

   // Create
   SegmentId id = segmentManager.createSegment(SegmentType::SP, 10);
   SPSegment& segment = segmentManager.getSPSegment(id);

   // Grow
   ASSERT_EQ(segment.numPages(), 10ul);
   segmentManager.growSegment(segment, 20ul);

   segmentManager.growSegment(segment, 10ul);
   ASSERT_EQ(segment.numPages(), 40ul);

   // Drop
   segmentManager.dropSegment(segment);
   SegmentId id_b = segmentManager.createSegment(SegmentType::SP, 98);
   SPSegment& segment_b = segmentManager.getSPSegment(id_b);
   ASSERT_EQ(segment_b.numPages(), 98ul);
}

TEST(SegmentManager, PersistentSISingle)
{
   const uint32_t pages = 100;
   assert(kSwapFilePages>=pages);

   SegmentId sid1;
   TupleId tid;

   // Create
   {
      // Add one 10 page segment
      BufferManager bufferManager(kSwapFileName, pages / 2);
      SegmentManager segmentManager(bufferManager, true);
      sid1 = segmentManager.createSegment(SegmentType::SP, 10);
      SPSegment& segment1 = segmentManager.getSPSegment(sid1);
      tid = segment1.insert(Record("Experience is simply the name we give our mistakes - Oscar Wilde"));
   }

   // Restart
   {
      // Check that the 10 page segment is still there
      BufferManager bufferManager(kSwapFileName, pages / 2);
      SegmentManager segmentManager(bufferManager, false);
      SPSegment& segment1 = segmentManager.getSPSegment(sid1);
      ASSERT_EQ(segment1.lookup(tid), Record("Experience is simply the name we give our mistakes - Oscar Wilde"));
   }
}

TEST(SegmentManager, PersistentSIList)
{
   const uint32_t pages = 4000;
   assert(kSwapFilePages>=pages);

   SegmentId sid1;
   SegmentId sid2;
   SegmentId sid3;

   // Create
   {
      BufferManager bufferManager(kSwapFileName, pages / 2);
      SegmentManager segmentManager(bufferManager, true);
      sid1 = segmentManager.createSegment(SegmentType::SP, 1);
      SPSegment& segment1 = segmentManager.getSPSegment(sid1);
      sid2 = segmentManager.createSegment(SegmentType::SP, 1);
      SPSegment& segment2 = segmentManager.getSPSegment(sid2);
      sid3 = segmentManager.createSegment(SegmentType::SP, 1);
      SPSegment& segment3 = segmentManager.getSPSegment(sid3);

      for(uint32_t i=0; i<kPageSize/16 - 12; i++) {
         segmentManager.growSegment(segment1, 1);
         segmentManager.growSegment(segment2, 1);
         segmentManager.growSegment(segment3, 1);
      }

      ASSERT_EQ(segment1.numPages(), kPageSize/16 - 11);
      ASSERT_EQ(segment2.numPages(), kPageSize/16 - 11);
      ASSERT_EQ(segment3.numPages(), kPageSize/16 - 11);
   }

   // Restart
   {
      BufferManager bufferManager(kSwapFileName, pages / 2);
      SegmentManager segmentManager(bufferManager, false);
      SPSegment& segment1 = segmentManager.getSPSegment(sid1);
      SPSegment& segment2 = segmentManager.getSPSegment(sid2);
      SPSegment& segment3 = segmentManager.getSPSegment(sid3);

      ASSERT_EQ(segment1.numPages(), kPageSize/16 - 11);
      ASSERT_EQ(segment2.numPages(), kPageSize/16 - 11);
      ASSERT_EQ(segment3.numPages(), kPageSize/16 - 11);
   }
}

TEST(SegmentManager, FunkeTest)
{
   const uint32_t pages = 1 * 1000;
   assert(kSwapFilePages>=pages);

   ASSERT_EQ(run(kSwapFileName, pages), 0);
}
