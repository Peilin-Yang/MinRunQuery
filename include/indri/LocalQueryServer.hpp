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
// LocalQueryServer
//
// 15 March 2004 -- tds
//
// Allows a QueryEnvironment to run queries against a
// local connection (without doing a network hop).
// This is especially useful for testing.
//

#ifndef INDRI_LOCALQUERYSERVER_HPP
#define INDRI_LOCALQUERYSERVER_HPP

#include "indri/QueryServer.hpp"
#include "indri/Repository.hpp"
#include "indri/ListCache.hpp"
#include "indri/InferenceNetwork.hpp"

namespace indri
{
  /*! \brief Indri query server classes. */
  namespace server
  {
    
    class LocalQueryServer : public QueryServer {
    private:
      // hold the value of the Parameter optimize, so only one call to
      // get is required. Globally disable query optimization if
      // the parameter is false.
      bool _optimizeParameter;
      indri::collection::Repository& _repository;
      indri::lang::ListCache _cache;

      //
      void _buildTermScoreFunction(
        indri::infnet::InferenceNetwork* network, 
        std::map<std::string, std::map<std::string, double> >& queryTerms, 
        std::map<std::string, double>& modelParas
      );
      void _buildScorerAccmulator();
    public:
      LocalQueryServer( indri::collection::Repository& repository );

      // query
      std::string processTerm( std::string s);
	    QueryServerResponse* getGlobalStatistics( std::vector<std::string>& queryTerms );
      QueryServerResponse* runQuery( std::map<std::string, std::map<std::string, double> >& queryTerms, 
        std::map<std::string, double>& modelParas, int resultsRequested, bool optimize );

      // single document queries
      std::string documentMetadatum( lemur::api::DOCID_T documentID, const std::string& attributeName );

      // batch queries
      QueryServerMetadataResponse* documentMetadata( const std::vector<lemur::api::DOCID_T>& documentIDs, const std::string& attributeName );
    };
  }
}

#endif // INDRI_LOCALQUERYSERVER_HPP
