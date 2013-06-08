#pragma once

#include "RelationSchema.hpp"
#include "harriet/ScriptLanguage.hpp"
#include "schema/Common.hpp"
#include <vector>
#include <string>

namespace dbi {

struct AttributeSignature {
   std::string name;
   std::string alias;
   bool notNull;
   bool primaryKey;
   harriet::VariableType type;
};

class Signature {
public:
   Signature();
   Signature(const RelationSchema& relationSchema, const std::string& alias); // Create named variables from a table scan
   Signature(const std::vector<std::unique_ptr<harriet::Value>>& values); // Deduce schema from an expression in a script

   Signature createProjectionSignature(const std::vector<ColumnIdentifier>& target) const;
   std::vector<uint32_t> createProjection(const std::vector<ColumnIdentifier>& target) const;

   const std::vector<AttributeSignature>& getAttributes() const;

private:
   uint32_t getAttribute(const std::string& alias, const std::string& name) const;
   std::vector<AttributeSignature> attributes;
};

}