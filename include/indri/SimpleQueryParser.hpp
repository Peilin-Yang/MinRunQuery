//
// SimpleQueryParserFactory
//
// 01/06/2016 Peilin Yang
//

#ifndef SIMPLEQUERYPARSER_HPP
#define SIMPLEQUERYPARSER_HPP
#include <string>
#include "lemur/Exception.hpp"
#include "indri/QuerySpec.hpp"

namespace indri
{
  namespace lang 
  {
    class ScoredExtentNode;
  }
  
  namespace api
  {
    // This class only deals with 1-level CombineNode and WeightedAndNode
    class SimpleQueryParser {
    private:
      std::string _rawQ;
    
    public:
      indri::lang::ScoredExtentNode* parseQuery( std::string query );
    };

    #define EMPTY_QUERY ((lemur::api::LemurErrorType)0xFFFFFFEF)

  }
}

#endif // SIMPLEQUERYPARSER_HPP
