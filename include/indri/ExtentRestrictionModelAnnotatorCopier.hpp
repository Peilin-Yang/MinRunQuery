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
// ExtentRestrictionModelAnnotatorCopier
//
// 16 August 2004 -- tds
//
// This copier annotates RawScorerNodes with the language models
// of the surrounding ExtentRestrictions.
//
// For example, the query #combine[sentence]( dog cat )
// should be synonymous with #combine[sentence]( dog.(sentence) cat.(sentence) ).
// We push the sentence language model down the tree and attach it to "dog" and "cat".
//

#ifndef INDRI_EXTENTRESTRICTIONMODELANNOTATORCOPIER_HPP
#define INDRI_EXTENTRESTRICTIONMODELANNOTATORCOPIER_HPP

#include <stack>
#include <vector>
#include "indri/QuerySpec.hpp"
namespace indri
{
  namespace lang
  {
    
    class ExtentRestrictionModelAnnotatorCopier : public indri::lang::Copier {
    private:
      std::vector<indri::lang::Node*> _nodes;
    public:
      ~ExtentRestrictionModelAnnotatorCopier() {
        indri::utility::delete_vector_contents( _nodes );
      }

      indri::lang::Node* defaultAfter( indri::lang::Node* old, indri::lang::Node* newNode ) {
        _nodes.push_back( newNode );
        return newNode;
      }
    };
  }
}

#endif // INDRI_EXTENTRESTRICTIONMODELANNOTATORCOPIER_HPP

