#include "indri/TermScoreFunction.hpp"
#include <cmath>

indri::query::TermScoreFunction::TermScoreFunction( double collectionOccurence, double collectionSize, double documentOccurrences, 
    double documentCount, std::map<std::string, double>& paras ) {
  _collectionOccurence = collectionOccurence;
  _collectionSize = collectionSize;
  _documentOccurrences = documentOccurrences;
  _documentCount = documentCount;
  _modelParas = paras;
  _modelParas["collectionFrequency"] = _collectionOccurence ? (_collectionOccurence/_collectionSize) : (1.0 / _collectionSize*2.);
  _modelParas["_muTimesCollectionFrequency"] = _modelParas["mu"] * _modelParas["collectionFrequency"];
}


double indri::query::TermScoreFunction::scoreOccurrence( double occurrences, int contextSize ) {
  double seen = ( double(occurrences) + _modelParas["_muTimesCollectionFrequency"] ) / ( double(contextSize) + _modelParas["mu"] );
  return log( seen );
}
