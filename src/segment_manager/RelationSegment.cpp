#include "RelationSegment.hpp"
#include "SegmentManager.hpp"
#include "schema/RelationSchema.hpp"
#include "harriet/Value.hpp"
#include "buffer_manager/BufferManager.hpp"
#include "BTreeSegment.hpp"
#include "btree/BTree.hpp"
#include "btree/IndexKey.hpp"
#include "btree/IndexKeyComparator.hpp"
#include "btree/IndexKeySchema.hpp"
#include <vector>

namespace dbi {
   
RelationSegment::RelationSegment(SegmentId id, FSISegment& fsi, SegmentInventory& si, BufferManager& bm, SegmentManager& sm, RelationSchema& rs)
:SPSegment(id, fsi, si, bm)
, segMan(sm)
, relSchema(rs)
{
}

RelationSegment::~RelationSegment()
{
   // TODO: drop related indices?
}

//arg: vector<Value>
TupleId RelationSegment::insert(const std::vector<harriet::Value>& tupleValues)
{
   TupleId tId = SPSegment::insert(relSchema.tupleToRecord(tupleValues));
   addRecordToIndices(tupleValues, tId);
   return tId;
}

void RelationSegment::remove(TupleId tId)
{
   Record recordToBeRemoved = lookup(tId);
   removeRecordFromIndices(relSchema.recordToTuple(recordToBeRemoved));
   SPSegment::remove(tId);
}

void RelationSegment::update(TupleId tId, const std::vector<harriet::Value>& tupleValues)
{
   // fetch old record and remove from indices
   Record oldRecord = this->lookup(tId);
   removeRecordFromIndices(relSchema.recordToTuple(oldRecord));
   
   SPSegment::update(tId, oldRecord);
   addRecordToIndices(tupleValues, tId);
}

// TODO: null checks per attr
void RelationSegment::addRecordToIndices(const std::vector<harriet::Value>& tupleValues, const TupleId& tId)
{  
   // validate uniqueness
   for(auto& index: relSchema.getIndexes()) {
      if(index.type != IndexSchema::Type::kBTree) {
         throw "Cannot add record to relation segment. Reason: unsupported index type " + index.type;
      }
      
      std::vector<harriet::Value*> keyColumns;
      std::vector<VariableType> keyTypes;
      
      for(auto colIndex : index.indexedColumns) {
         keyColumns.pushback(&tupleValues[colIndex]);
         keyTypes.push_back(&tupleValues[colIndex].type);
      }
      
      // Create index key
      IndexKey key(keyColumns);
      // Create index key schema
      IndexKeySchema keySchema(keyTypes);
      // Create index key comparator
      IndexKeyComparator keyComparator(keySchema);
      
      auto& bsm = segMan.getBTreeSegment(index.sid);
      BTree<IndexKey, IndexKeyComparator, IndexKeySchema> btree(bsm, keyComparator, keySchema);
      // Update the tree
      if(index.unique) {
         if(btree.lookup(key, tId)) {
            
         }
      }
      btree.insert(key, tId);
   }
}

void RelationSegment::removeRecordFromIndices(const std::vector<harriet::Value>& tupleValues)
{
   
}

struct IndexTreeUtil {
   
};

}