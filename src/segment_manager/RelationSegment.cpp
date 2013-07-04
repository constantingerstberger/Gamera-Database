#include "RelationSegment.hpp"
#include "SegmentManager.hpp"
#include "buffer_manager/BufferManager.hpp"
#include "BTreeSegment.hpp"
#include "btree/BTree.hpp"

namespace dbi {
   
RelationSegment::RelationSegment(SegmentId id, FSISegment& fsi, SegmentInventory& si, BufferManager& bm, SegmentManager& sm)
:SPSegment(id, fsi, si, bm)
, segMan(sm)
, relSchema(nullptr)
{
}

RelationSegment::~RelationSegment()
{
   // TODO: drop related indices?
}

TupleId RelationSegment::insert(const std::vector<harriet::Value>& tupleValues)
{
   auto indexData = createCompositors(tupleValues);
   auto indices = relSchema->getIndexes();
   validateUniqueConstraints(indexData, indices, kInvalidTupleId);
   TupleId tId = SPSegment::insert(relSchema->tupleToRecord(tupleValues));
   addRecordToIndices(indexData, indices, tId);
   return tId;
}

void RelationSegment::remove(TupleId tId)
{
   auto indices = relSchema->getIndexes();
   auto indexData = createCompositors(relSchema->recordToTuple(lookup(tId)));
   removeRecordFromIndices(indexData, indices);
   SPSegment::remove(tId);
}

// TODO: check if tId was unused before or is invalid
void RelationSegment::update(TupleId tId, const std::vector<harriet::Value>& tupleValues)
{
   auto indices = relSchema->getIndexes();
   // Check if new tuple values violate a unique constraint
   auto newIndexData = createCompositors(tupleValues);
   validateUniqueConstraints(newIndexData, indices, tId);
   // Fetch old record and remove from indices
   auto oldIndexData = createCompositors(relSchema->recordToTuple(lookup(tId)));
   removeRecordFromIndices(oldIndexData, indices);
   // Update actual record
   SPSegment::update(tId, relSchema->tupleToRecord(tupleValues));
   // Update indices
   addRecordToIndices(newIndexData, indices, tId);
}

void RelationSegment::addRecordToIndices(const std::vector<IndexKeyCompositor>& indexKeyData, const std::vector<IndexSchema>& indices, const TupleId tId)
{  
   for(uint8_t i = 0; i < indices.size(); i++) {
         auto& bsm = segMan.getBTreeSegment(indices[i].sid);
         BTree<IndexKey, IndexKeyComparator, IndexKeySchema> btree(bsm, indexKeyData[i].keyComparator, indexKeyData[i].keySchema);
         btree.insert(indexKeyData[i].key, tId);
   } 
}

void RelationSegment::removeRecordFromIndices(const std::vector<IndexKeyCompositor>& indexKeyData, const std::vector<IndexSchema>& indices)
{
   for(uint8_t i = 0; i < indices.size(); i++) {
         auto& bsm = segMan.getBTreeSegment(indices[i].sid);
         BTree<IndexKey, IndexKeyComparator, IndexKeySchema> btree(bsm, indexKeyData[i].keyComparator, indexKeyData[i].keySchema);
         btree.erase(indexKeyData[i].key);
   } 
}

void RelationSegment::validateUniqueConstraints(const std::vector<IndexKeyCompositor>& indexKeyData, const std::vector<IndexSchema>& indices, const TupleId& tId) 
{
   for(uint8_t i = 0; i < indices.size(); i++) {
      if(indices[i].unique) {
         auto& bsm = segMan.getBTreeSegment(indices[i].sid);
         BTree<IndexKey, IndexKeyComparator, IndexKeySchema> btree(bsm, indexKeyData[i].keyComparator, indexKeyData[i].keySchema);
         TupleId res = btree.lookup(indexKeyData[i].key);
         if((tId == kInvalidTupleId && res != kInvalidTupleId) || (tId != kInvalidTupleId && res != kInvalidTupleId && tId != res))
            throw harriet::Exception("unique constraint validation");
      }  
   } 
}

std::vector<RelationSegment::IndexKeyCompositor> RelationSegment::createCompositors(const std::vector<harriet::Value>& tupleValues)
{
   std::vector<IndexKeyCompositor> res;
   
   for(auto& index: relSchema->getIndexes()) {
      if(index.type != IndexSchema::Type::kBTree)
         throw "Cannot add record to relation segment. Reason: unsupported index type " + index.type;
      if(!index.unique)
         throw "non-unique indices not yet supported";
   
      std::vector<const harriet::Value*> keyColumns;
      std::vector<harriet::VariableType> keyTypes;
      
      for(auto colIndex : index.indexedColumns) {
         keyColumns.push_back(&tupleValues[colIndex]);
         keyTypes.push_back(tupleValues[colIndex].type);
      }
      res.push_back(IndexKeyCompositor(IndexKey(keyColumns), IndexKeySchema(keyTypes), IndexKeyComparator(IndexKeySchema(keyTypes))));
   }
   return res;
}

}