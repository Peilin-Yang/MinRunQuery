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
// NetworkServerStub
//
// 21 May 2004 -- tds
//

#include "indri/NetworkServerStub.hpp"
#include "indri/XMLNode.hpp"
#include "indri/NetworkMessageStream.hpp"
#include "indri/QueryServer.hpp"
#include "indri/QueryResponsePacker.hpp"
#include "indri/ParsedDocument.hpp"
#include "lemur/Exception.hpp"

indri::net::NetworkServerStub::NetworkServerStub( indri::server::QueryServer* server, indri::net::NetworkMessageStream* stream ) :
  _server(server),
  _stream(stream)
{
}

void indri::net::NetworkServerStub::_handleQuery( indri::xml::XMLNode* request ) {
  indri::lang::Unpacker unpacker(request);
  std::vector<indri::lang::Node*> nodes = unpacker.unpack();
  int resultsRequested = (int) string_to_i64( request->getAttribute( "resultsRequested" ) );
  bool optimize = request->getAttribute("optimize") == "1";

  indri::server::QueryServerResponse* response = _server->runQuery( nodes, resultsRequested, optimize );
  indri::infnet::InferenceNetwork::MAllResults results = response->getResults();

  QueryResponsePacker packer( results );
  packer.write( _stream );
  delete response;
}

void indri::net::NetworkServerStub::_handleDocumentMetadata( indri::xml::XMLNode* request ) {
  std::vector<lemur::api::DOCID_T> documentIDs;
  std::string fieldAttributeName = "field";
  std::string field = request->getChild( fieldAttributeName )->getValue();

  const indri::xml::XMLNode* documents = request->getChild("documents");

  for( size_t i=0; i<documents->getChildren().size(); i++ ) {
    documentIDs.push_back( (lemur::api::DOCID_T) string_to_i64( documents->getChildren()[i]->getValue() ) );
  }

  // get the documents
  indri::server::QueryServerMetadataResponse* metadataResponse = _server->documentMetadata( documentIDs, field );
  std::vector<std::string> metadata = metadataResponse->getResults();
  delete metadataResponse;

  // send them back
  indri::xml::XMLNode* response = new indri::xml::XMLNode( "document-metadata" );
  for( size_t i=0; i<metadata.size(); i++ ) {
    std::string value = base64_encode( metadata[i].c_str(), (int)metadata[i].length() );
    indri::xml::XMLNode* datum = new indri::xml::XMLNode( "datum", value );
    response->addChild(datum);
  }

  _stream->reply( response );
  _stream->replyDone();
  delete response;
}

void indri::net::NetworkServerStub::request( indri::xml::XMLNode* input ) {
  try {
    const std::string& type = input->getName();

    if( type == "query" ) {
      _handleQuery( input );
    } else if( type == "document-metadata" ) {
      _handleDocumentMetadata( input );
    } else {
      _stream->error( std::string() + "Unknown XML message type: " + input->getName() );
    }
  } catch( lemur::api::Exception& e ) {
    _stream->error( e.what() );
  } catch( ... ) {
    _stream->error( "Caught unknown exception while processing request" );
  }
}

void indri::net::NetworkServerStub::reply( indri::xml::XMLNode* input ) {
  assert( false && "Shouldn't ever get a reply on the server" );
}

void indri::net::NetworkServerStub::reply( const std::string& name, const void* buffer, unsigned int length ) {
  assert( false && "Shouldn't ever get a reply on the server" );
}

void indri::net::NetworkServerStub::replyDone() {
  assert( false && "Shouldn't ever get a reply on the server" );
}

void indri::net::NetworkServerStub::error( const std::string& error ) {
  // TODO: fix this to trap the error and propagate up the chain on the next request.
  std::cout << "Caught error message from client: " << error.c_str() << std::endl;
}

void indri::net::NetworkServerStub::run() {
  while( _stream->alive() )
    _stream->read(*this);
}
