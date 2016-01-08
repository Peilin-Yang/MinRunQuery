//
// SimpleQueryParserFactory
//
// 01/06/2016 Peilin Yang
//

#include <sstream>
#include <algorithm>
#include "indri/SimpleQueryParser.hpp"
#include "lemur/Exception.hpp"

void split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
  		if (!item.empty()) {
  		  elems.push_back(item);
  		}
    }
}

std::vector<std::string> split(const std::string &s, char delim=' ') {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

indri::lang::WeightNode* parseWAnd(std::vector<std::string>& qv) {
  indri::lang::WeightNode* wan = new indri::lang::WeightNode();
  for (size_t i = 1; i < qv.size(); i += 2) {
    wan->addChild(qv[i], new indri::lang::RawScorerNode(new indri::lang::IndexTerm(qv[i+1]), 0));
  }
  return wan;
}

indri::lang::ScoredExtentNode* indri::api::SimpleQueryParser::parseQuery( std::string query ) {
  std::replace( query.begin(), query.end(), '(', ' ');
  std::replace( query.begin(), query.end(), ')', ' ');
  //query.erase(std::remove_if(query.begin(), query.end(), isspace), query.end());
  std::vector<std::string> query_vector = split(query);
  if (query_vector.empty()) 
    LEMUR_THROW( EMPTY_QUERY, "Query Cannot Be Empty!" );
  if (query_vector[0] == "#weight") {
    return parseWAnd(query_vector);
  } else if (query_vector[0] == "#combine") {

  } else {

  }
  return new indri::lang::WeightNode();
}

