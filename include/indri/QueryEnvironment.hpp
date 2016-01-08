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
#include "indri/NetworkStream.hpp"
#include "indri/NetworkMessageStream.hpp"
#include "indri/Parameters.hpp"
#include "indri/ParsedDocument.hpp"
#include "indri/Repository.hpp"
#include "lemur/IndexTypes.hpp"

namespace indri 
{

  namespace api 
  {
    /*! \brief Structure for aggregating a query and its parameters.
      Provides an aggregate for specifying a query an retrieval parameters
      as a single argument. Can include parameters for snippet generation,
      metadata field retrieval, number of results, starting number of the
      results, and a working set of documents to evaluate the query on.
    */
    typedef struct QueryRequest 
    {
      /*! Snippet generation options.
       */
      enum Options {
        /// Generate an html snippet with matches in &lt;strong&gt; tags.
        HTMLSnippet = 1,
        /// Generate a text snippet with matches in upper case.
        TextSnippet = 2
      };
      /// the query to run
      std::string query;
      /// the list of metadata fields to return
      std::vector<std::string>  metadata;
      /// the working set of documents to evaluate the query against, uses internal document ids.
      std::vector<lemur::api::DOCID_T>  docSet;
      /// number of results to return
      int resultsRequested;
      /// starting number in the result set, eg 10 to get results starting at the 11th position in the result list.
      int startNum;
      /// snippet generation options
      enum Options options;
    } QueryRequest;

    /*! encapsulation of a metadata field and its value
     */
    typedef struct MetadataPair 
    {
      /// the metadata field name
      std::string key;
      /// the value of the field
      std::string value;
    } MetadataPair;
    
    /*! \brief Structure for aggregating a query result.
      Provides an aggregate for a query result based an retrieval parameters
      from a QueryRequest. Can include a text snippet, external document id,
      internal document id, metadata field values, score, extent begin, and
      extent end.
    */
    typedef struct QueryResult
    {
      /// text snippet generated by SnippetBuilder for this result
      std::string snippet;
      /// external document id
      std::string documentName;
      /// internal document id
      lemur::api::DOCID_T docid;
      /// query score
      double score;
      /// extent begin
      int begin;
      /// extent end
      int end;
      /// list of retrieved metadata fields
      std::vector<indri::api::MetadataPair> metadata;
    } QueryResult;

    /*! Aggretate of the list of QueryResult elements for a QueryRequest,
      with estimated number of total matches, query parse time, query
      query execution time, and parsed document processing time 
      (metadata retrieval and snippet generation).
     */
    typedef struct QueryResults 
    {
      /// time to parse the query in milliseconds
      float parseTime;
      /// time to evaluate the query in milliseconds
      float executeTime;
      /// time to retrieve metadata fields and generate snippets
      float documentsTime;
      /// estimated number of matches for the query
      int estimatedMatches;
      /// the list of QueryResult elements.
      std::vector<QueryResult> results;
    } QueryResults;
    
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
      std::map<std::string, std::pair<indri::server::QueryServer *, indri::net::NetworkStream *> > _serverNameMap;
      std::vector<indri::server::QueryServer*> _servers;
      // first is entry _servers, second is entry in _repositories.
      // derive idx to erase from those.
      std::map<std::string, std::pair<indri::server::QueryServer *, indri::collection::Repository *> > _repositoryNameMap;
      std::vector<indri::collection::Repository*> _repositories;
      std::vector<indri::net::NetworkStream*> _streams;
      std::vector<indri::net::NetworkMessageStream*> _messageStreams;

      Parameters _parameters;
      
      void _mergeQueryResults( indri::infnet::InferenceNetwork::MAllResults& results, std::vector<indri::server::QueryServerResponse*>& responses );
      void _copyStatistics( std::vector<indri::lang::RawScorerNode*>& scorerNodes, indri::infnet::InferenceNetwork::MAllResults& statisticsResults );

      std::vector<indri::server::QueryServerResponse*> _runServerQuery( std::vector<indri::lang::Node*>& roots, int resultsRequested );
      void _sumServerQuery( indri::infnet::InferenceNetwork::MAllResults& results, std::vector<indri::lang::Node*>& roots, int resultsRequested );
      void _mergeServerQuery( indri::infnet::InferenceNetwork::MAllResults& results, std::vector<indri::lang::Node*>& roots, int resultsRequested );
     
      std::vector<indri::api::ScoredExtentResult> _runQuery( indri::infnet::InferenceNetwork::MAllResults& results,
                                                             const std::string& q,
                                                             int resultsRequested,
                                                             const std::string &queryType = "indri" );
      void _scoredQuery( indri::infnet::InferenceNetwork::MAllResults& results, indri::lang::Node* queryRoot, std::string& accumulatorName, int resultsRequested );

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
