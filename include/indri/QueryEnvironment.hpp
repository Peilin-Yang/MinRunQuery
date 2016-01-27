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
// QueryEnvironment
//
// 9 March 2004 -- tds
//

#ifndef INDRI_QUERYENVIRONMENT_HPP
#define INDRI_QUERYENVIRONMENT_HPP

#include <map>
#include "indri/ScoredExtentResult.hpp"
#include "indri/QueryServer.hpp"
#include "indri/Parameters.hpp"
#include "indri/ParsedDocument.hpp"
#include "indri/Repository.hpp"
#include "lemur/IndexTypes.hpp"

namespace indri 
{

  namespace api 
  {
    /*! encapsulation of a metadata field and its value
     */
    typedef struct MetadataPair 
    {
      /// the metadata field name
      std::string key;
      /// the value of the field
      std::string value;
    } MetadataPair;
    
    typedef struct QueryDict 
    {
      std::string processed; // processed term, e.g. stemmed
      double qtf; // query term frequency
      double collectionFrequency; // number of occurences in the collection of the term
      double collTermCnt; // total number of terms in collection
      int docFrequency; // number of documents we occur in
      int docCnt; // total number of documents
    } QueryDict;
      

    /*! \brief Principal class for interacting with Indri indexes during retrieval. 
      Provides the API for opening one or more Repository servers, either local
      or remote. Provides the API for querying the servers with the Indri
      query language, and additionally requesting aggregate collection 
      statistics.
    */
    class QueryEnvironment {
    private:
      // first is entry in _servers, second is entry _streams
      // derive idx to erase from those.
      std::vector<indri::server::QueryServer*> _servers;
      // first is entry _servers, second is entry in _repositories.
      // derive idx to erase from those.
      std::map<std::string, std::pair<indri::server::QueryServer *, indri::collection::Repository *> > _repositoryNameMap;
      std::vector<indri::collection::Repository*> _repositories;

      // we parse the query to the query dict where key is the raw query string and value is its QTF
      std::map<std::string, QueryDict> _queryDict;
      // mapping processedTerm->rawQueryTerm
      std::map<std::string, std::string> _reverseMapping;
      // model parameters. This is read from the parameter file/commandline
      std::map<std::string, double> _modelParas;

      Parameters _parameters;
      
      void _setQTF(std::map<std::string, double>& parsedQuery);
      void _transformQuery();
      std::vector<std::string> _getProcessedQTerms();
      std::map<std::string, std::map<std::string, double> > _getProcessedQTermswithStats();
      void _setCollectionStatistics( indri::infnet::InferenceNetwork::MAllResults& statisticsResults );
      void _mergeQueryResults( indri::infnet::InferenceNetwork::MAllResults& results, std::vector<indri::server::QueryServerResponse*>& responses );
      std::vector<indri::server::QueryServerResponse*> _runServerQuery( int resultsRequested );
      void _sumServerQuery( indri::infnet::InferenceNetwork::MAllResults& results, int resultsRequested );
      void _mergeServerQuery( indri::infnet::InferenceNetwork::MAllResults& results, int resultsRequested );
     
      std::vector<indri::api::ScoredExtentResult> _runQuery( indri::infnet::InferenceNetwork::MAllResults& results,
                                                             const std::string& q,
                                                             int resultsRequested,
                                                             const std::string &queryType = "indri" );
      void _scoredQuery( indri::infnet::InferenceNetwork::MAllResults& results, int resultsRequested );

      QueryEnvironment( QueryEnvironment& other ) {}

    public:
      QueryEnvironment();
      ~QueryEnvironment();
      /// \brief Set the amount of memory to use.
      /// @param memory number of bytes to allocate
      void setMemory( UINT64 memory );
      void setSingleBackgroundModel( bool background );
      /// \brief Set the scoring rules
      /// @param rules the vector of scoring rules.
      void setScoringRules( const std::vector<std::string>& rules );
      /// \brief Set the stopword list for query processing
      /// @param stopwords the list of stopwords
      void setStopwords( const std::vector<std::string>& stopwords );
      /// \brief Add a remote server
      /// @param hostname the host the server is running on
      void addServer( const std::string& hostname );
      /// \brief Add a local repository
      /// @param pathname the path to the repository.
      void addIndex( const std::string& pathname );
      /// Close the QueryEnvironment.
      void close();

      /// \brief Run an Indri query language query. @see ScoredExtentResult
      /// @param query the query to run
      /// @param resultsRequested maximum number of results to return
      /// @return the vector of ScoredExtentResults for the query
      std::vector<indri::api::ScoredExtentResult> runQuery( const std::string& query, int resultsRequested, const std::string &queryType = "indri" );

      /// \brief Run an Indri query language query. @see ScoredExtentResult
      /// @param query the query to run
      /// @param documentSet the working set of document ids to evaluate
      /// @param resultsRequested maximum number of results to return
      /// @return the vector of ScoredExtentResults for the query
      std::vector<indri::api::ScoredExtentResult> runQuery( const std::string& query, const std::vector<lemur::api::DOCID_T>& documentSet, int resultsRequested, const std::string &queryType = "indri" );

      /// \brief Fetch the named metadata attribute for a list of document ids
      /// @param documentIDs the list of ids
      /// @param attributeName the name of the metadata attribute
      /// @return the vector of string values for that attribute
      std::vector<std::string> documentMetadata( const std::vector<lemur::api::DOCID_T>& documentIDs, const std::string& attributeName );
      /// \brief Fetch the named metadata attribute for a list of ScoredExtentResults
      /// @param documentIDs the list of ScoredExtentResults
      /// @param attributeName the name of the metadata attribute
      /// @return the vector of string values for that attribute
      std::vector<std::string> documentMetadata( const std::vector<indri::api::ScoredExtentResult>& documentIDs, const std::string& attributeName );
    };
  }
}

#endif // INDRI_QUERYENVIRONMENT_HPP

