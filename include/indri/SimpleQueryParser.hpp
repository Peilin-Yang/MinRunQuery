//
// SimpleQueryParserFactory
//
// 01/06/2016 Peilin Yang
//

#ifndef SIMPLEQUERYPARSER_HPP
#define SIMPLEQUERYPARSER_HPP
#include <string>
#include <map>
#include "lemur/Exception.hpp"
#include "indri/Parameters.hpp"

namespace indri
{
  namespace query
  {
    // This class only deals with 1-level CombineNode and WeightedAndNode
    class SimpleQueryParser {
    private:
      std::string _rawQ;
    
    public:
      // split the query string. return a Dict with the key as the unique query term 
      // and the value as the counts(qtf) in the query.
      std::map<std::string, double> parseQuery( std::string query );

      // process the query terms. the input is the result of "parseQuery".
      // For example, we can stem the query terms.
      std::map<std::string, double> processQueryTerms( std::map<std::string, double>& queryDict );

      // load the possible parameters for the method
      void loadModelParameters( indri::api::Parameters& parameters, std::map<std::string, double>& res );
    };

    #define EMPTY_QUERY ((lemur::api::LemurErrorType)0xFFFFFFEF)
    #define PARSE_MODEL_PARA_ERR ((lemur::api::LemurErrorType)0xFFFFFFEE)
  }
}

#endif // SIMPLEQUERYPARSER_HPP
