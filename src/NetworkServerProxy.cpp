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
// NetworkServerProxy
//
// 20 May 2004 -- tds
//

#include "indri/NetworkServerProxy.hpp"
#include "indri/ParsedDocument.hpp"
#include "indri/ScopedLock.hpp"
#include <iostream>

namespace indri
{
  namespace server
  {
    
    //
    // NetworkServerProxyResponse
    //
    class NetworkServerProxyResponse : public QueryServerResponse {
    private:
      indri::net::QueryResponseUnpacker _unpacker;
      indri::net::NetworkMessageStream* _stream;

    public:
      NetworkServerProxyResponse( indri::net::NetworkMessageStream* stream ) :
        _unpacker( stream ),
        _stream( stream )
      {
      }

      ~NetworkServerProxyResponse() {
        _stream->mutex().unlock();
      }

      indri::infnet::InferenceNetwork::MAllResults& getResults() {
        return _unpacker.getResults();
      }
    };

    //
    // NetworkServerProxyMetadataResponse
    //

    class NetworkServerProxyMetadataResponse : public QueryServerMetadataResponse {
    private:
      std::vector<std::string> _metadata;
      indri::net::NetworkMessageStream* _stream;

    public:
      NetworkServerProxyMetadataResponse( indri::net::NetworkMessageStream* stream ) :
        _stream(stream)
      {
      }

      ~NetworkServerProxyMetadataResponse() {
        _stream->mutex().unlock();
      }

      std::vector<std::string>& getResults() {
        indri::net::XMLReplyReceiver r;
        r.wait(_stream);

        // parse the result
        indri::xml::XMLNode* reply = r.getReply();
        indri::utility::Buffer metadataBuffer;

        for( size_t i=0; i<reply->getChildren().size(); i++ ) {
          const indri::xml::XMLNode* meta = reply->getChildren()[i];
          const std::string& input = meta->getValue();

          std::string value;
          base64_decode_string( value, input );
          _metadata.push_back(value);

          metadataBuffer.clear();
        }

        return _metadata;
      }
    };
  }
}

//
// NetworkServerProxy code
//


indri::server::NetworkServerProxy::NetworkServerProxy( indri::net::NetworkMessageStream* stream ) :
  _stream(stream)
{
}

//
// _numericRequest
//
// Sends a request for a numeric quantity; deletes the node
// passed in as a parameter
//

INT64 indri::server::NetworkServerProxy::_numericRequest( indri::xml::XMLNode* node ) {
  indri::thread::ScopedLock lock( _stream->mutex() );
  _stream->request( node );
  delete node;

  indri::net::XMLReplyReceiver r;
  r.wait( _stream );

  indri::xml::XMLNode* reply = r.getReply();
  return string_to_i64( reply->getValue() );
}

//
// _stringRequest
//
// Sends a request for a string quantity; deletes the node
// passed in as a parameter
//

std::string indri::server::NetworkServerProxy::_stringRequest( indri::xml::XMLNode* node ) {
  indri::thread::ScopedLock lock( _stream->mutex() );
  _stream->request( node );
  delete node;

  indri::net::XMLReplyReceiver r;
  r.wait( _stream );

  indri::xml::XMLNode* reply = r.getReply();
  return reply->getValue();
}

//
// runQuery
//

indri::server::QueryServerResponse* indri::server::NetworkServerProxy::runQuery( std::vector<indri::lang::Node*>& roots, int resultsRequested, bool optimize ) {
  indri::lang::Packer packer;

  for( size_t i=0; i<roots.size(); i++ ) {
    packer.pack( roots[i] );
  }

  indri::xml::XMLNode* query = packer.xml();
  query->addAttribute( "resultsRequested", i64_to_string(resultsRequested) );
  query->addAttribute( "optimize", optimize ? "1" : "0" );

  _stream->mutex().lock();
  _stream->request( query );

  return new indri::server::NetworkServerProxyResponse( _stream );
}

//
// documentMetadata
//

indri::server::QueryServerMetadataResponse* indri::server::NetworkServerProxy::documentMetadata( const std::vector<lemur::api::DOCID_T>& documentIDs, const std::string& attributeName ) {
  indri::xml::XMLNode* request = new indri::xml::XMLNode( "document-metadata" );
  indri::xml::XMLNode* field = new indri::xml::XMLNode( "field", attributeName );
  indri::xml::XMLNode* documents = new indri::xml::XMLNode( "documents" );

  // build request
  for( size_t i=0; i<documentIDs.size(); i++ ) {
    documents->addChild( new indri::xml::XMLNode( "document", i64_to_string( documentIDs[i] ) ) );
  }
  request->addChild( field );
  request->addChild( documents );

  // send request
  _stream->mutex().lock();
  _stream->request( request );
  delete request;

  return new indri::server::NetworkServerProxyMetadataResponse( _stream );
}