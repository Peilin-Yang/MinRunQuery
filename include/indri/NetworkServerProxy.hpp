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
// 23 March 2004 -- tds
//

#ifndef INDRI_NETWORKSERVERPROXY_HPP
#define INDRI_NETWORKSERVERPROXY_HPP

#include "indri/XMLNode.hpp"
#include "indri/QueryResponseUnpacker.hpp"
#include "indri/QueryServer.hpp"
#include "indri/Packer.hpp"
#include "indri/NetworkMessageStream.hpp"
#include "indri/Buffer.hpp"
namespace indri
{
  namespace server
  {   
    class NetworkServerProxy : public QueryServer {
    private:
      indri::net::NetworkMessageStream* _stream;

      INT64 _numericRequest( indri::xml::XMLNode* node );
      std::string _stringRequest( indri::xml::XMLNode* node );

    public:
      NetworkServerProxy( indri::net::NetworkMessageStream* stream );

      QueryServerResponse* runQuery( std::vector<indri::lang::Node*>& roots, int resultsRequested, bool optimize );
      QueryServerMetadataResponse* documentMetadata( const std::vector<lemur::api::DOCID_T>& documentIDs, const std::string& attributeName );
    };
  }
}

#endif // INDRI_NETWORKSERVERPROXY_HPP
