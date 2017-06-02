#include "indri/TermScoreFunction.hpp"
#include <cmath>
#include <iostream>

using namespace std;

indri::query::TermScoreFunction::TermScoreFunction( double collectionOccurence, 
    double collectionSize, double documentOccurrences, double documentCount, 
    double avdl, double queryLength, std::map<std::string, double>& paras ) {
  _collectionOccurence = collectionOccurence;
  _collectionSize = collectionSize;
  _documentOccurrences = documentOccurrences;
  _documentCount = documentCount;
  _avdl = avdl;
  _modelParas = paras;
  _modelParas["mu"] = 2500;
  _modelParas["collectionFrequency"] = _collectionOccurence ? (_collectionOccurence/_collectionSize) : (1.0 / _collectionSize*2.);
  _modelParas["_muTimesCollectionFrequency"] = _modelParas["mu"] * _modelParas["collectionFrequency"];
}


double indri::query::TermScoreFunction::scoreOccurrence( double occurrences, int contextSize, double qtf, double docUniqueTerms ) {
  double seen = ( double(occurrences) + _modelParas["_muTimesCollectionFrequency"] ) / ( double(contextSize) + _modelParas["mu"] );
  return log( seen );
}
