//
// SimpleQueryParserFactory
//
// 01/06/2016 Peilin Yang
//

#include <sstream>
#include <algorithm>
#include <vector>
#include "indri/SimpleQueryParser.hpp"

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

std::map<string, double> indri::query::SimpleQueryParser::parseQuery( std::string query ) {
  std::replace( query.begin(), query.end(), '(', ' ');
  std::replace( query.begin(), query.end(), ')', ' ');
  //query.erase(std::remove_if(query.begin(), query.end(), isspace), query.end());
  std::vector<std::string> query_vector = split(query);
  if (query_vector.empty()) 
    LEMUR_THROW( EMPTY_QUERY, "Query Cannot Be Empty!" );
  std::map<string, double> parsed;
  for (size_t i = 0; i < query_vector.size(); i++) {
    std::string cur = query_vector[i];
    if (parsed.find(cur) == parsed.end()) {
      parsed[cur] = 1.0;
    } else {
      parsed[cur] += 1.0;
    }
  }
  return parsed;
}

