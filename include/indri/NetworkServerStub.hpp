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
// 23 March 2004 -- tds
//

#ifndef INDRI_NETWORKSERVERSTUB_HPP
#define INDRI_NETWORKSERVERSTUB_HPP

#include "indri/XMLNode.hpp"
#include "indri/NetworkMessageStream.hpp"
#include "indri/QueryServer.hpp"
#include "indri/QueryResponsePacker.hpp"
namespace indri
{
  namespace net
  {
    
    class NetworkServerStub : public MessageStreamHandler {
    private:
      indri::server::QueryServer* _server;
      NetworkMessageStream* _stream;

      void _handleDocumentMetadata( indri::xml::XMLNode* request );
      void _handleQuery( indri::xml::XMLNode* input );
    public:
      NetworkServerStub( indri::server::QueryServer* server, NetworkMessageStream* stream );
      void request( indri::xml::XMLNode* input );
      void reply( indri::xml::XMLNode* input );
      void reply( const std::string& name, const void* buffer, unsigned int length );
      void replyDone();
      void error( const std::string& error );
      void run();
    };
  }
}

#endif // INDRI_NETWORKSERVERSTUB_HPP
