#include "CodeGenerationVisitor.hpp"
#include "Statement.hpp"
#include <sstream>
#include <iomanip>

using namespace std;

namespace dbi {

namespace script {

CodeGenerationVisitor::CodeGenerationVisitor(std::ostream& out)
: out(out)
, indention(0)
{
}

CodeGenerationVisitor::~CodeGenerationVisitor()
{
}

void CodeGenerationVisitor::onPreVisit(RootStatement&)
{
   const std::string header = R"AAA(
// This code was generated by the gamera database system

#include "core/TransactionCallbackHandler.hpp"
#include "schema/RelationSchema.hpp"
#include <iostream>
#include <cstdint>

#ifdef __cplusplus
   extern "C" {
#endif

void entry(dbi::TransactionCallbackHandler& database))AAA";
   out << header << endl;
}

void CodeGenerationVisitor::onPostVisit(RootStatement&)
{
   const std::string footer = R"AAA(
#ifdef __cplusplus
   }
#endif)AAA";
   out << footer << endl;
}

void CodeGenerationVisitor::onPreVisit(SelectStatement& select)
{
   throw;
}

void CodeGenerationVisitor::onPostVisit(SelectStatement&)
{
   throw;
}

void CodeGenerationVisitor::onPreVisit(CreateTableStatement& createTable)
{
   out << string(indention*3, ' ') << "dbi::RelationSchema schema;" << endl;
   out << string(indention*3, ' ') << "schema.name = \"" << createTable.name << "\";" << endl;

   for(auto& iter : createTable.attributes)
      out << string(indention*3, ' ') << "schema.attributes.push_back(dbi::AttributeSchema{\"" << iter.name << "\", harriet::nameToType(\"" << iter.type << "\"), false, true});" << endl;

   out << string(indention*3, ' ') << "database.createTable(schema);" << endl;
}

void CodeGenerationVisitor::onPostVisit(CreateTableStatement&)
{
   throw;
}

void CodeGenerationVisitor::onPreVisit(InsertStatement& insert)
{
   throw;
}

void CodeGenerationVisitor::onPostVisit(InsertStatement& insert)
{
}

void CodeGenerationVisitor::onPreVisit(BlockStatement&)
{
   out << string(indention*3, ' ') << "{" << endl;
   indention++;
}

void CodeGenerationVisitor::onPostVisit(BlockStatement&)
{
   indention--;
   out << string(indention*3, ' ') << "}" << endl;
}

}

}
