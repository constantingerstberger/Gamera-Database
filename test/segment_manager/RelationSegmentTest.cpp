#include "gtest/gtest.h"
#include "buffer_manager/BufferManager.hpp"
#include "common/Config.hpp"
#include "segment_manager/SegmentManager.hpp"
#include "segment_manager/RelationSegment.hpp"
#include "util/Utility.hpp"
#include "test/TestConfig.hpp"
#include "harriet/VariableType.hpp"
#include "schema/RelationSchema.hpp"
#include <vector>

using namespace std;
using namespace dbi;
using namespace harriet;

ColumnSchema createColSchema(string name, VariableType vt, uint16_t offset) 
{
   return ColumnSchema {
      name, vt, true, offset
   };
}

vector<Value> createTuple(int32_t id, string name, float grade, bool like){
   vector<Value> res;
   res.push_back(Value::createInteger(id, false));
   res.push_back(Value::createCharacter(name, 10, false));
   res.push_back(Value::createFloat(grade, false));
   res.push_back(Value::createBool(like, false));
   return res;
}

IndexSchema createIndexSchema(SegmentId segId, vector<uint8_t> inCols, IndexSchema::Type type, bool unique)
{
   return IndexSchema {
      segId, inCols, type, unique
   };
}


TEST(RelationSegment, RelationSegmentSimple) 
{
   const uint32_t pages = 100;
   assert(kSwapFilePages>=pages);

   // Create segment
   BufferManager bufferManager(kSwapFileName, pages / 2);
   SegmentManager segmentManager(bufferManager, true);
   SegmentId id = segmentManager.createSegment(SegmentType::RE, 10);
   RelationSegment& relation = segmentManager.getRelationSegment(id);
   
   // Create column schemas
   const uint16_t nameLength = 10;
   vector<ColumnSchema> columns;
   ColumnSchema cs0 = createColSchema("id", VariableType::createIntegerType(), 0);
   ColumnSchema cs1 = createColSchema("name", VariableType::createCharacterType(nameLength), cs0.type.length);
   ColumnSchema cs2 = createColSchema("grade", VariableType::createFloatType(), cs1.offset + cs1.type.length);
   ColumnSchema cs3 = createColSchema("likes_c++", VariableType::createBoolType(), cs2.offset + cs2.type.length);
   columns.push_back(cs0);
   columns.push_back(cs1);
   columns.push_back(cs2);
   columns.push_back(cs3);
   
   // Create index segment 
   SegmentId treeId = segmentManager.createSegment(SegmentType::BT, 10);
   
   // Create index schemas
   vector<uint8_t> inCols;
   inCols.push_back(0);
   vector<IndexSchema> indices;
   indices.push_back(createIndexSchema(treeId, inCols, IndexSchema::kBTree, true));
   
   // Create relation schema
   const string relationName = "asdf";
   RelationSchema rs(relationName, move(columns), move(indices));
   rs.setSegmentId(id);
   
   relation.setRelationSchema(&rs);
   
   
   // Insert and retrieve test
   TupleId tubbleId = relation.insert(createTuple(1, "alex", 5.0, true));
   vector<Value> resTubble = rs.recordToTuple(relation.lookup(tubbleId));
   ASSERT_EQ(resTubble[0].data.vint, 1);
   ASSERT_EQ(resTubble[1].str(), "alex");
   ASSERT_EQ(resTubble[2].data.vfloat, 5.0);
   ASSERT_EQ(resTubble[3].data.vbool, true);
   
   // validate tree
   ASSERT_THROW(relation.insert(createTuple(1, "cons", 1.0, false)), harriet::Exception);
   
}


