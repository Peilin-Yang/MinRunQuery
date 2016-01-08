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


#ifndef INDRI_QUERYSPEC_HPP
#define INDRI_QUERYSPEC_HPP

#include <vector>
#include <string>
#include <sstream>
#include <indri/greedy_vector>
#include <algorithm>
#include "lemur/lemur-platform.h"

#include "indri/Walker.hpp"
#include "indri/Copier.hpp"
#include "indri/Packer.hpp"
#include "indri/Unpacker.hpp"

#include "lemur/Exception.hpp"
#include "indri/HashTable.hpp"
#include "indri/ref_ptr.hpp"

template<class T>
bool equal( const std::vector<T>& one, const std::vector<T>& two ) {
  if( one.size() != two.size() )
    return false;

  for( size_t i=0; i<one.size(); i++ ) {
    if( *one[i] == *two[i] )
      continue;

    return false;
  }

  return true;
}

template<class T>
bool unordered_equal( std::vector<T>& one, std::vector<T>& two ) {
  if( one.size() != two.size() )
    return false;

  std::vector<T> one_copy;
  for( size_t i=0; i<one.size(); i++ ) {
    one_copy.push_back( one[i] );
  }

  // this algorithm is n^2 as opposed to n log n if
  // we sorted things, but windows tend to be short
  for( size_t i=0; i<two.size(); i++ ) {
    for( size_t j=0; j<one_copy.size(); j++ ) {
      if( *one_copy[j] == *two[i] ) {
        // we remove each match--if they all match, the array will be empty
        one_copy.erase( one_copy.begin() + j );
        break;
      }
    }
  }

  return one_copy.size() == 0;
}

namespace indri {
  namespace lang {
    /* abstract */ class Node {
    protected:
      std::string _name;

    public:
      Node() {
        std::stringstream s;
        s << PTR_TO_INT(this);
        _name = s.str();
      }

      virtual ~Node() {
      }
      
      void setNodeName( const std::string& name ) {
        _name = name;
      }

      const std::string& nodeName() const {
        return _name;
      }

      virtual std::string typeName() const {
        return "Node";
      }

      virtual std::string queryText() const = 0;

      virtual bool operator < ( Node& other ) {
        // TODO: make this faster
        if( typeName() != other.typeName() )
          return typeName() < other.typeName();

        return queryText() < other.queryText();
      }
     
      virtual bool operator== ( Node& other ) {
        return &other == this; 
      }

      virtual UINT64 hashCode() const = 0;
      virtual void pack( Packer& packer ) = 0;
      virtual void walk( Walker& walker ) = 0;
      virtual Node* copy( Copier& copier ) = 0;
    };

    /* abstract */ class RawExtentNode : public Node {};
    /* abstract */ class ScoredExtentNode : public Node {};
    /* abstract */ class AccumulatorNode : public Node {};
    
    class IndexTerm : public RawExtentNode {
    private:
      std::string _text;
      bool _stemmed;

    public:
      IndexTerm( const std::string& text, bool stemmed = false ) : _text(text), _stemmed(stemmed)
      {
      }

      IndexTerm( Unpacker& unpacker ) {
        _text = unpacker.getString( "termName" );
        _stemmed = unpacker.getBoolean( "stemmed" );
      }

      const std::string& getText() { return _text; }

      bool operator==( Node& node ) {
        IndexTerm* other = dynamic_cast<IndexTerm*>(&node);

        if( !other )
          return false;

        if( other == this )
          return true;
        
        return other->_text == _text;
      }

      std::string typeName() const {
        return "IndexTerm";
      }

      std::string queryText() const {
        std::stringstream qtext;

        if( _stemmed ) {
          qtext << '"' << _text << '"';
        } else {
          qtext << _text;
        }

        return qtext.str();
      }

      void setStemmed(bool stemmed) {
        _stemmed = stemmed;
      }

      bool getStemmed() const {
        return _stemmed;
      }

      UINT64 hashCode() const {
        int accumulator = 1;

        if( _stemmed )
          accumulator += 3;

        indri::utility::GenericHash<const char*> hash;
        return accumulator + hash( _text.c_str() );
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "termName", _text );
        packer.put( "stemmed", _stemmed );
        packer.after(this);
      }

      void walk( Walker& walker ) {
        walker.before(this);
        walker.after(this);
      }

      Node* copy( Copier& copier ) {
        copier.before(this);
        IndexTerm* termCopy = new IndexTerm(*this);
        return copier.after(this, termCopy);
      }
    };

    class RawScorerNode : public ScoredExtentNode {
    protected:
      double _occurrences; // number of occurrences within this context
      double _contextSize; // number of terms that occur within this context
      double _maximumContextFraction;
      int _documentOccurrences; // number of documents we occur in
      int _documentCount; // total number of documents

      RawExtentNode* _raw;
      RawExtentNode* _context;
      std::string _smoothing;

    public:
      RawScorerNode( RawExtentNode* raw, RawExtentNode* context, std::string smoothing = "method:dirichlet,mu:2500" ) {
        _raw = raw;
        _context = context;

        _occurrences = 0;
        _contextSize = 0;
        _documentOccurrences = 0;
        _documentCount = 0;
        _smoothing = smoothing;
      }

      RawScorerNode( Unpacker& unpacker ) {
        _raw = unpacker.getRawExtentNode( "raw" );
        _context = unpacker.getRawExtentNode( "context" );

        _occurrences = unpacker.getDouble( "occurrences" );
        _contextSize = unpacker.getDouble( "contextSize" );
        _documentOccurrences = unpacker.getInteger( "documentOccurrences" );
        _documentCount = unpacker.getInteger( "documentCount" );
        _smoothing = unpacker.getString( "smoothing" );
      }

      virtual std::string typeName() const {
        return "RawScorerNode";
      }

      std::string queryText() const {
        std::stringstream qtext;
        
        qtext << _raw->queryText();
        if( _context ) {
          // if we haven't added a period yet, put one in
          int dot = (int)qtext.str().find('.');
          if( dot < 0 )
            qtext << '.';

          qtext << "(" << _context->queryText() << ")";
        }

        return qtext.str();
      }

      virtual UINT64 hashCode() const {
        UINT64 hash = 0;

        hash += 43;
        hash += _raw->hashCode();

        if( _context ) {
          hash += _context->hashCode();
        }

        indri::utility::GenericHash<const char*> gh;
        hash += gh( _smoothing.c_str() );

        return hash;
      }

      double getOccurrences() const {
        return _occurrences;
      }

      double getContextSize() const {
        return _contextSize;
      }

      int getDocumentOccurrences() const {
        return _documentOccurrences;
      }

      int getDocumentCount() const {
        return _documentCount;
      }
      
      const std::string& getSmoothing() const {
        return _smoothing;
      }

      void setStatistics( double occurrences, double contextSize, int documentOccurrences, int documentCount ) {
        _occurrences = occurrences;
        _contextSize = contextSize;
        _documentOccurrences = documentOccurrences;
        _documentCount = documentCount;
      }

      void setContext( RawExtentNode* context ) {
        _context = context;
      }

      void setRawExtent( RawExtentNode* rawExtent ) {
        _raw = rawExtent;
      }

      void setSmoothing( const std::string& smoothing ) {
        _smoothing = smoothing;
      }

      RawExtentNode* getContext() {
        return _context;
      }

      RawExtentNode* getRawExtent() {
        return _raw;
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "raw", _raw );
        packer.put( "context", _context );

        packer.put( "occurrences", _occurrences );
        packer.put( "contextSize", _contextSize );
        packer.put( "documentOccurrences", _documentOccurrences );
        packer.put( "documentCount", _documentCount );
        packer.put( "smoothing", _smoothing );
        packer.after(this);
      }

      void walk( Walker& walker ) {
        walker.before(this);
        if( _raw )
          _raw->walk(walker);
        if( _context )
          _context->walk(walker);
        walker.after(this);
      }

      virtual Node* copy( Copier& copier ) {
        copier.before(this);

        RawExtentNode* duplicateContext = _context ? dynamic_cast<RawExtentNode*>(_context->copy(copier)) : 0;
        RawExtentNode* duplicateRaw = _raw ? dynamic_cast<RawExtentNode*>(_raw->copy(copier)) : 0;
        RawScorerNode* duplicate = new RawScorerNode(*this);
        duplicate->setRawExtent( duplicateRaw );
        duplicate->setContext( duplicateContext );

        return copier.after(this, duplicate);
      }
    };

    class TermFrequencyScorerNode : public ScoredExtentNode {
    private:
      double _occurrences; // number of occurrences within this context
      double _contextSize; // number of terms that occur within this context
      int _documentOccurrences; // number of documents we occur in
      int _documentCount; // total number of documents

      std::string _text;
      std::string _smoothing;
      bool _stemmed;

    public:
      TermFrequencyScorerNode( const std::string& text, bool stemmed ) {
        _occurrences = 0;
        _contextSize = 0;
        _documentOccurrences = 0;
        _documentCount = 0;
        _smoothing = "";
        _text = text;
        _stemmed = stemmed;
      }

      TermFrequencyScorerNode( Unpacker& unpacker ) {
        _occurrences = unpacker.getDouble( "occurrences" );
        _contextSize = unpacker.getDouble( "contextSize" );
        _documentOccurrences = unpacker.getInteger( "documentOccurrences" );
        _documentCount = unpacker.getInteger( "documentCount" );
        _smoothing = unpacker.getString( "smoothing" );
        _text = unpacker.getString( "text" );
        _stemmed = unpacker.getBoolean( "stemmed" );
      }
      
      const std::string& getText() const {
        return _text;
      }

      bool getStemmed() const {
        return _stemmed;
      }

      std::string typeName() const {
        return "TermFrequencyScorerNode";
      }

      std::string queryText() const {
        std::stringstream qtext;
        
        if( !_stemmed )
          qtext << _text;
        else
          qtext << "\"" << _text << "\"";

        return qtext.str();
      }

      UINT64 hashCode() const {
        int accumulator = 47;

        if( _stemmed )
          accumulator += 3;

        indri::utility::GenericHash<const char*> hash;
        return accumulator + hash( _text.c_str() ) * 7 + hash( _smoothing.c_str() );
      }

      double getOccurrences() const {
        return _occurrences;
      }

      double getContextSize() const {
        return _contextSize;
      }

      int getDocumentOccurrences() const {
        return _documentOccurrences;
      }

      int getDocumentCount() const {
        return _documentCount;
      }

      const std::string& getSmoothing() const {
        return _smoothing;
      }

      void setStatistics( double occurrences, double contextSize, int documentOccurrences, int documentCount ) {
        _occurrences = occurrences;
        _contextSize = contextSize;
        _documentOccurrences = documentOccurrences;
        _documentCount = documentCount;
      }

      void setSmoothing( const std::string& smoothing ) {
        _smoothing = smoothing;
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "occurrences", _occurrences );
        packer.put( "contextSize", _contextSize );
        packer.put( "documentOccurrences", _documentOccurrences );
        packer.put( "documentCount", _documentCount );
        packer.put( "text", _text );
        packer.put( "stemmed", _stemmed );
        packer.put( "smoothing", _smoothing );
        packer.after(this);
      }

      void walk( Walker& walker ) {
        walker.before(this);
        walker.after(this);
      }

      Node* copy( Copier& copier ) {
        copier.before(this);
        TermFrequencyScorerNode* duplicate = new TermFrequencyScorerNode(*this);
        return copier.after(this, duplicate);
      }
    };

    /* abstract */ class UnweightedCombinationNode : public ScoredExtentNode {
    protected:
      std::vector<ScoredExtentNode*> _children;

      void _unpack( Unpacker& unpacker ) {
        _children = unpacker.getScoredExtentVector( "children" );
      }

      UINT64 _hashCode() const {
        UINT64 accumulator = 0;

        for( size_t i=0; i<_children.size(); i++ ) {
          accumulator += _children[i]->hashCode();
        }

        return accumulator;
      }

      template<class _ThisType>
      void _walk( _ThisType* ptr, Walker& walker ) {
        walker.before(ptr);

        for( size_t i=0; i<_children.size(); i++ ) {
          _children[i]->walk(walker);
        }
        
        walker.after(ptr);
      }

      template<class _ThisType>
      Node* _copy( _ThisType* ptr, Copier& copier ) {
        copier.before(ptr);
        
        _ThisType* duplicate = new _ThisType();
        duplicate->setNodeName( nodeName() );
        for( size_t i=0; i<_children.size(); i++ ) {
          duplicate->addChild( dynamic_cast<ScoredExtentNode*>(_children[i]->copy(copier)) );
        } 

        return copier.after(ptr, duplicate);
      }

      void _childText( std::stringstream& qtext ) const {
        for( size_t i=0; i<_children.size(); i++ ) {
          if(i>0) qtext << " ";
          qtext << _children[i]->queryText();
        }
      }

    public:
      const std::vector<ScoredExtentNode*>& getChildren() {
        return _children;
      }

      void addChild( ScoredExtentNode* scoredNode ) {
        _children.push_back( scoredNode );
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "children", _children );
        packer.after(this);
      }
    };

    /* abstract */ class WeightedCombinationNode : public ScoredExtentNode {
    protected:
      std::vector< std::pair<double, ScoredExtentNode*> > _children;

      void _unpack( Unpacker& unpacker ) {
        std::vector<double> weights = unpacker.getDoubleVector( "weights" );
        std::vector<ScoredExtentNode*> nodes = unpacker.getScoredExtentVector( "children" );

        for( size_t i=0; i<weights.size(); i++ ) {
          _children.push_back( std::make_pair( weights[i], nodes[i] ) );
        }
      }

      UINT64 _hashCode() const {
        UINT64 accumulator = 0;

        for( size_t i=0; i<_children.size(); i++ ) {
          accumulator += (UINT64) (_children[i].first * 1000) + _children[i].second->hashCode();
        }

        return accumulator;
      }

      template<class _ThisType>
      void _walk( _ThisType* ptr, Walker& walker ) {
        walker.before(ptr);
        for( size_t i=0; i<_children.size(); i++ ) {
          _children[i].second->walk(walker);
        }
        walker.after(ptr);
      }

      template<class _ThisType>
      Node* _copy( _ThisType* ptr, Copier& copier ) {
        copier.before(ptr);

        _ThisType* duplicate = new _ThisType;
        duplicate->setNodeName( nodeName() );
        for( size_t i=0; i<_children.size(); i++ ) {
          double childWeight = _children[i].first;
          Node* childCopy = _children[i].second->copy( copier );

          duplicate->addChild( childWeight, dynamic_cast<ScoredExtentNode*>(childCopy) );
        }
        return copier.after(ptr, duplicate);
      }

      void _childText( std::stringstream& qtext ) const {
        for( size_t i=0; i<_children.size(); i++ ) {
          if(i>0) qtext << " ";
          qtext << _children[i].first
                << " "
                << _children[i].second->queryText();
        }
      }

    public:
      const std::vector< std::pair<double, ScoredExtentNode*> >& getChildren() {
        return _children;
      }

      void addChild( double weight, ScoredExtentNode* scoredNode ) {
        _children.push_back( std::make_pair( weight, scoredNode) );
      }

      void addChild( const std::string& weight, ScoredExtentNode* scoredNode ) {
        addChild( atof( weight.c_str() ), scoredNode );
      }

      void pack( Packer& packer ) {
        packer.before(this);
        
        std::vector<double> weights;
        std::vector<ScoredExtentNode*> nodes;

        for( size_t i=0; i<_children.size(); i++ ) {
          weights.push_back( _children[i].first );
          nodes.push_back( _children[i].second );
        }

        packer.put( "weights", weights );
        packer.put( "children", nodes );
        packer.after(this);
      }
    };

    class CombineNode : public UnweightedCombinationNode {
    public:
      CombineNode() {}
      CombineNode( Unpacker& unpacker ) {
        _unpack( unpacker );
      }

      std::string typeName() const {
        return "CombineNode";
      }

      std::string queryText() const {
        std::stringstream qtext;
        qtext << "#combine(";
        _childText(qtext);
        qtext << ")";

        return qtext.str();
      } 

      UINT64 hashCode() const {
        return 59 + _hashCode();
      }

      void walk( Walker& walker ) {
        _walk( this, walker );
      }
      
      Node* copy( Copier& copier ) {
        return _copy( this, copier );
      }
    };

    class WeightNode : public WeightedCombinationNode {
    public:
      WeightNode() {}
      WeightNode( Unpacker& unpacker ) {
        _unpack( unpacker );
      }

      std::string typeName() const {
        return "WeightNode";
      }

      std::string queryText() const {
        std::stringstream qtext;
        qtext << "#weight(";
        _childText(qtext);
        qtext << ")";

        return qtext.str();
      }

      UINT64 hashCode() const {
        return 71 + _hashCode();
      }

      void walk( Walker& walker ) {
        _walk( this, walker );
      }

      Node* copy( Copier& copier ) {
        return _copy( this, copier );
      }
    };

    class ContextCounterNode : public AccumulatorNode {
    private:
      RawExtentNode* _raw;
      RawExtentNode* _context;
      bool _hasCounts;
      bool _hasContextSize;
      double _occurrences;
      double _contextSize;
      int _documentOccurrences; // number of documents we occur in
      int _documentCount; // total number of documents

    public:
      ContextCounterNode( RawExtentNode* raw, RawExtentNode* context ) :
        _hasCounts(false),
        _hasContextSize(false),
        _occurrences(0),
        _contextSize(0),
        _documentOccurrences(0),
        _documentCount(0)
      {
        _raw = raw;
        _context = context;
      }

      ContextCounterNode( Unpacker& unpacker ) {
        _raw = unpacker.getRawExtentNode( "raw" );
        _context = unpacker.getRawExtentNode( "context" );
        _occurrences = unpacker.getDouble( "occurrences" );
        _contextSize = unpacker.getDouble( "contextSize" );
        _documentOccurrences = unpacker.getInteger( "documentOccurrences" );
        _documentCount = unpacker.getInteger( "documentCount" );

        _hasCounts = unpacker.getBoolean( "hasCounts" );
        _hasContextSize = unpacker.getBoolean( "hasContextSize" );
      }

      std::string typeName() const {
        return "ContextCounterNode";
      }

      std::string queryText() const {
        std::stringstream qtext;
        
        if( _raw )
          qtext << _raw->queryText();

        if( _context ) {
          // if we haven't added a period yet, put one in
          int dot = (int)qtext.str().find('.');
          if( dot < 0 )
            qtext << '.';

          qtext << "(" << _context->queryText() << ")";
        }

        return qtext.str();
      }

      UINT64 hashCode() const {
        // we don't use hashCodes for accumulatorNodes
        return 0;
      }

      RawExtentNode* getContext() {
        return _context;
      }

      RawExtentNode* getRawExtent() {
        return _raw;
      }

      void setRawExtent( RawExtentNode* rawExtent ) {
        _raw = rawExtent;
      }

      void setContext( RawExtentNode* context ) {
        _context = context;
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "raw", _raw );
        packer.put( "context", _context );
        packer.put( "occurrences", _occurrences );
        packer.put( "contextSize", _contextSize );
        packer.put( "documentOccurrences", _documentOccurrences );
        packer.put( "documentCount", _documentCount );

        packer.put( "hasCounts", _hasCounts );
        packer.put( "hasContextSize", _hasContextSize );
        packer.after(this);
      }

      void walk( Walker& walker ) {
        walker.before(this);
        if( _raw ) _raw->walk(walker);
        if( _context ) _context->walk(walker);
        walker.after(this);
      }

      Node* copy( Copier& copier ) {
        copier.before(this);
        RawExtentNode* duplicateRaw = _raw ? dynamic_cast<RawExtentNode*>(_raw->copy(copier)) : 0;
        RawExtentNode* duplicateContext = _context ? dynamic_cast<RawExtentNode*>(_context->copy(copier)) : 0;
        ContextCounterNode* duplicate = new ContextCounterNode(*this);
        duplicate->setContext(duplicateContext);
        duplicate->setRawExtent(duplicateRaw);
        return copier.after(this, duplicate);
      }

      bool hasCounts() const {
        return _hasCounts;
      }

      bool hasContextSize() const {
        return _hasContextSize;
      }

      double getOccurrences() const {
        return _occurrences;
      }

      double getContextSize() const {
        return _contextSize;
      }

      int getDocumentOccurrences() const {
        return _documentOccurrences;
      }

      int getDocumentCount() const {
        return _documentCount;
      }

      void setContextSize( double contextSize ) {
        _contextSize = contextSize;
        _hasContextSize = true;
      }

      void setCounts( double occurrences,
                      double contextSize, int documentOccurrences, 
                      int documentCount ) {
        _hasCounts = true;
        _occurrences = occurrences;
        setContextSize( contextSize );
        _documentOccurrences = documentOccurrences;
        _documentCount = documentCount;
      }
    };

    class ContextSimpleCounterNode : public AccumulatorNode {
    private:
      std::vector<std::string> _terms;
      std::string _field;
      std::string _context;

      bool _hasCounts;
      bool _hasContextSize;
      double _occurrences;
      double _contextSize;
      int _documentOccurrences; // number of documents we occur in
      int _documentCount; // total number of documents

    public:
      ContextSimpleCounterNode( const std::vector<std::string>& terms, const std::string& field, const std::string& context ) :
        _hasCounts(false),
        _hasContextSize(false),
        _occurrences(0),
        _contextSize(0),
        _terms(terms),
        _field(field),
        _context(context),
        _documentOccurrences(0),
        _documentCount(0)
      {
      }

      ContextSimpleCounterNode( Unpacker& unpacker ) {
        _occurrences = unpacker.getDouble( "occurrences" );
        _contextSize = unpacker.getDouble( "contextSize" );

        _terms = unpacker.getStringVector( "terms" );
        _field = unpacker.getString( "field" );
        _context = unpacker.getString( "context" );
        _documentOccurrences = unpacker.getInteger( "documentOccurrences" );
        _documentCount = unpacker.getInteger( "documentCount" );

        _hasCounts = unpacker.getBoolean( "hasCounts" );
        _hasContextSize = unpacker.getBoolean( "hasContextSize" );
      }

      std::string typeName() const {
        return "ContextSimpleCounterNode";
      }

      std::string queryText() const {
        // nothing to see here -- this is an optimization node
        return std::string();
      }

      UINT64 hashCode() const {
        // we don't use hashCodes for accumulatorNodes
        return 0;
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "occurrences", _occurrences );
        packer.put( "contextSize", _contextSize );
        packer.put( "documentOccurrences", _documentOccurrences );
        packer.put( "documentCount", _documentCount );

        packer.put( "terms", _terms );
        packer.put( "field", _field );
        packer.put( "context", _context );

        packer.put( "hasCounts", _hasCounts );
        packer.put( "hasContextSize", _hasContextSize );
        packer.after(this);
      }

      void walk( Walker& walker ) {
        walker.before(this);
        walker.after(this);
      }

      Node* copy( Copier& copier ) {
        copier.before(this);
        ContextSimpleCounterNode* duplicate = new ContextSimpleCounterNode(*this);
        return copier.after(this, duplicate);
      }

      bool hasCounts() const {
        return _hasCounts;
      }

      bool hasContextSize() const {
        return _hasContextSize;
      }

      double getOccurrences() const {
        return _occurrences;
      }

      double getContextSize() const {
        return _contextSize;
      }

      int getDocumentOccurrences() const {
        return _documentOccurrences;
      }

      int getDocumentCount() const {
        return _documentCount;
      }

      const std::vector<std::string>& terms() const {
        return _terms;
      }

      const std::string& field() const {
        return _field;
      }

      const std::string& context() const {
        return _context;
      }

      void setContextSize( double contextSize ) {
        _contextSize = contextSize;
        _hasContextSize = true;
      }

      void setCounts( double occurrences,
                      double contextSize, int documentOccurrences, 
                      int documentCount ) {
        _hasCounts = true;
        _occurrences = occurrences;
        setContextSize( contextSize );
        _documentOccurrences = documentOccurrences;
        _documentCount = documentCount;
      }
    };

    class ScoreAccumulatorNode : public AccumulatorNode {
    private:
      ScoredExtentNode* _scoredNode;

    public:
      ScoreAccumulatorNode( ScoredExtentNode* scoredNode ) :
        _scoredNode(scoredNode)
      {
      }

      ScoreAccumulatorNode( Unpacker& unpacker ) {
        _scoredNode = unpacker.getScoredExtentNode( "scoredNode" );
      }

      std::string typeName() const {
        return "ScoreAccumulatorNode";
      }

      std::string queryText() const {
        // anonymous
        return _scoredNode->queryText();
      }

      UINT64 hashCode() const {
        // we don't use hashCodes for accumulatorNodes
        return 0;
      }

      ScoredExtentNode* getChild() {
        return _scoredNode;
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "scoredNode", _scoredNode );
        packer.after(this);
      }

      void walk( Walker& walker ) {
        walker.before(this);
        _scoredNode->walk(walker);
        walker.after(this);
      }

      Node* copy( Copier& copier ) {
        copier.before(this);
        ScoredExtentNode* duplicateChild = dynamic_cast<ScoredExtentNode*>(_scoredNode->copy(copier));
        ScoreAccumulatorNode* duplicate = new ScoreAccumulatorNode(duplicateChild);
        duplicate->setNodeName( nodeName() );
        return copier.after(this, duplicate);
      }
    };

    class ContextInclusionNode : public ScoredExtentNode {
    protected:
      std::vector<ScoredExtentNode*> _children;
      ScoredExtentNode* _preserveExtentsChild;

      void _unpack( Unpacker& unpacker ) {
        _children = unpacker.getScoredExtentVector( "children" );
        _preserveExtentsChild = unpacker.getScoredExtentNode( "preserveExtentsChild" );
      }

      UINT64 _hashCode() const {
        UINT64 accumulator = 0;

        for( size_t i=0; i<_children.size(); i++ ) {
          accumulator += _children[i]->hashCode();
        }

        return accumulator;
      }

      template<class _ThisType>
      void _walk( _ThisType* ptr, Walker& walker ) {
        walker.before(ptr);

        for( size_t i=0; i<_children.size(); i++ ) {
          _children[i]->walk(walker);
        }
        
        walker.after(ptr);
      }

      template<class _ThisType>
      Node* _copy( _ThisType* ptr, Copier& copier ) {
        copier.before(ptr);
        
        _ThisType* duplicate = new _ThisType();
        duplicate->setNodeName( nodeName() );
        for( size_t i=0; i<_children.size(); i++ ) {
          bool preserveExtents = false;
          if ( _preserveExtentsChild == _children[i] ) {
            preserveExtents = true;
          }
          duplicate->addChild( dynamic_cast<ScoredExtentNode*>(_children[i]->copy(copier)), preserveExtents );
        } 

        return copier.after(ptr, duplicate);
      }

      void _childText( std::stringstream& qtext ) const {
        if ( _preserveExtentsChild != 0 ) {
          qtext << _preserveExtentsChild->queryText() << " ";
        }
        for( size_t i=0; i<_children.size(); i++ ) {
          if ( _children[i] != _preserveExtentsChild ) {
            if(i>0) qtext << " ";
            qtext << _children[i]->queryText();
          }
        }
      }

    public:
      ContextInclusionNode( ) { }
      ContextInclusionNode( Unpacker & unpacker ) {
        _unpack( unpacker );
      }

      const std::vector<ScoredExtentNode*>& getChildren() {
        return _children;
      }
      
      ScoredExtentNode * getPreserveExtentsChild() {
        return _preserveExtentsChild;
      }

      void addChild( ScoredExtentNode* scoredNode, bool preserveExtents = false ) {
        if (preserveExtents == true) {
          _preserveExtentsChild = scoredNode;
        }       
        _children.push_back( scoredNode );
      }

      std::string typeName() const {
        return "ContextInclusionNode";
      }

      std::string queryText() const {
        std::stringstream qtext;
        qtext << "#context(";
        _childText(qtext);
        qtext << ")";

        return qtext.str();
      } 

      virtual UINT64 hashCode() const {
        return 111 + _hashCode();//?????????????
      }

      void pack( Packer& packer ) {
        packer.before(this);
        packer.put( "children", _children );
        packer.put( "preserveExtentsChild", _preserveExtentsChild);
        packer.after(this);
      }

      void walk( Walker& walker ) {
        _walk( this, walker );
      }
      
      Node* copy( Copier& copier ) {
        return _copy( this, copier );
      }
    };

  }
}

#endif // INDRI_QUERYSPEC_HPP
