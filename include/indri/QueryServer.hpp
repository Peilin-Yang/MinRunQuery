/*==========================================================================
 * Copyright (c) 2004 University of Massachusetts.  All Rights Reserved.
 *
 * Use of the Lemur Toolkit for Language Modeling and Information Retrieval
 * is subject to the terms of the software license set forth in the LICENSE
 * file included with this software, and also available at
 * http://www.lemurproject.org/license.html
 *
 *==========================================================================
 */


//
// QueryServer
//
// 15 March 2004 -- tds
//

#ifndef INDRI_QUERYSERVER_HPP
#define INDRI_QUERYSERVER_HPP

#include "indri/QuerySpec.hpp"
#include "indri/InferenceNetwork.hpp"
#include "lemur/IndexTypes.hpp"
#include <vector>
namespace indri
{
  namespace server
  {
    
    class QueryServerResponse {
    public:
      virtual ~QueryServerResponse() {};
      virtual indri::infnet::InferenceNetwork::MAllResults& getResults() = 0;
    };

    class QueryServerMetadataResponse {
    public:
      virtual ~QueryServerMetadataResponse() {};
      virtual std::vector<std::string>& getResults() = 0;
    };

    class QueryServer {
    public:
      virtual ~QueryServer() {};
	  virtual QueryServerResponse* getGlobalStatistics( std::map<std::string, double>& queryDict ) = 0;
      virtual QueryServerResponse* runQuery( std::map<std::string, double>& queryDict, int resultsRequested, bool optimize ) = 0;
      virtual QueryServerMetadataResponse* documentMetadata( const std::vector<lemur::api::DOCID_T>& documentIDs, const std::string& attributeName ) = 0;
    };
  }
}

#endif // INDRI_QUERYSERVER_HPP

