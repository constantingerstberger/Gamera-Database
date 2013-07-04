#pragma once

#include "SPSegment.hpp"
#include "harriet/Value.hpp"
#include "btree/IndexKey.hpp"
#include "btree/IndexKeySchema.hpp"
#include "btree/IndexKeyComparator.hpp"
#include "schema/RelationSchema.hpp"
#include <vector>

namespace dbi {
   
class SegmentManager;
class RelationSchema;
   
class RelationSegment : public SPSegment {
public:
   RelationSegment(SegmentId id, FSISegment& fsi, SegmentInventory& si, BufferManager& bm, SegmentManager& sm);
   virtual ~RelationSegment();
   
   // TODO: overwrite these: -> update index ... somehow
   TupleId insert(const std::vector<harriet::Value>& tupleValues);
   void remove(TupleId tId);
   void update(TupleId tId, const std::vector<harriet::Value>& tupleValues);
   void setRelationSchema(const RelationSchema* rs) {
      relSchema = rs;
   }
   
private:
   SegmentManager& segMan;
   const RelationSchema* relSchema;
   
   struct IndexKeyCompositor {
      
      IndexKeyCompositor(IndexKey&& key, IndexKeySchema&& keySchema, const IndexKeyComparator& keyComparator): key(std::move(key)), keySchema(std::move(keySchema)), keyComparator(keyComparator) 
      {
      }
      
      IndexKey key;
      IndexKeySchema keySchema;
      IndexKeyComparator keyComparator;
   };
   
   void addRecordToIndices(const std::vector<IndexKeyCompositor>& indexKeyData, const std::vector<IndexSchema>& indices, const TupleId tId);
   
   void removeRecordFromIndices(const std::vector<IndexKeyCompositor>& indexKeyData, const std::vector<IndexSchema>& indices);
   
   std::vector<IndexKeyCompositor> createCompositors(const std::vector<harriet::Value>& tupleValues);
   
   /**
    * Checks for every unique index if the key contained in the respective indexKeyCompositor is already associated with a value.
    * If so, a harriet::Exception is thrown, as this is a unique key violation. However, if a tId != kInvalidTupleId is provided, 
    * the exception is only thrown in case the value obtained from the index (which is a tuple id) is not kInvalidTupleId and 
    * does not match the provided tId.
    * @param indexKeyData
    * @param indices
    * @param tId
    */
   void validateUniqueConstraints(const std::vector<IndexKeyCompositor>& indexKeyData, const std::vector<IndexSchema>& indices, const TupleId& tId);

};

}