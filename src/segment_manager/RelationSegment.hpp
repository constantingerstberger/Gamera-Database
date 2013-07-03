#pragma once

#include "SPSegment.hpp"
#include "harriet/Value.hpp"

namespace dbi {
   
class SegmentManager;
class RelationSchema;
   
class RelationSegment : public SPSegment {
public:
   RelationSegment(SegmentId id, FSISegment& fsi, SegmentInventory& si, BufferManager& bm, SegmentManager& sm, RelationSchema& rs);
   virtual ~RelationSegment();
   
   // TODO: overwrite these: -> update index ... somehow
   TupleId insert(const std::vector<harriet::Value>& tupleValues);
   void remove(TupleId tId);
   void update(TupleId tId, const std::vector<harriet::Value>& tupleValues);
   
private:
   SegmentManager& segMan;
   RelationSchema& relSchema;
   
   void addRecordToIndices(const std::vector<harriet::Value>& tupleValues);
   
   void removeRecordFromIndices(const std::vector<harriet::Value>& tupleValues);

};

}