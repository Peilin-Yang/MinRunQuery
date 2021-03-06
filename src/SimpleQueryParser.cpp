//
// SimpleQueryParserFactory
//
// 01/06/2016 Peilin Yang
//

#include <sstream>
#include <algorithm>
#include <vector>
#include <cstdlib>
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


void indri::query::SimpleQueryParser::loadModelParameters( 
    indri::api::Parameters& parameters, 
    std::map<std::string, double>& res ) {
  if( !parameters.exists("rule") ) { return; }
  indri::api::Parameters rules = parameters["rule"];
  if (rules.size() == 0) { return; }
  size_t x = 0;

  std::vector<std::string> para_vectors = split(rules[x], ',');
  for (size_t i = 0; i < para_vectors.size(); i++) {
    std::string cur = para_vectors[i];
    try {
      std::vector<std::string> this_para = split(cur, ':');
      res[this_para.at(0)] = atof(this_para.at(1).c_str());
    }
    catch (...) {
      LEMUR_THROW( EMPTY_QUERY, "Parse Model Parameters Error!" );
    }
  }
}

void indri::query::SimpleQueryParser::loadPertubeParameters( 
    const int pertube_type,
    const std::map<std::string, double>& pertube_paras,
    std::map<std::string, double>& res ) {
  res["__PERTUBE_TYPE__"] = pertube_type;
  for (std::map<std::string, double>::const_iterator it=pertube_paras.begin(); it!=pertube_paras.end(); ++it) {
    res["__PERTUBE_"+it->first+"__"] = it->second;
  }
}
