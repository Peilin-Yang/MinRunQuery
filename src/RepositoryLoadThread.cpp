/*==========================================================================
 * Copyright (c) 2005 University of Massachusetts.  All Rights Reserved.
 *
 * Use of the Lemur Toolkit for Language Modeling and Information Retrieval
 * is subject to the terms of the software license set forth in the LICENSE
 * file included with this software, and also available at
 * http://www.lemurproject.org/license.html
 *
 *==========================================================================
 */

//
// RepositoryLoadThread
//
// 31 January 2005 -- tds
//

#include "indri/RepositoryLoadThread.hpp"
#include "indri/Repository.hpp"

const int FIVE_SECONDS = 5*1000*1000;
const int HALF_SECOND = 500*1000;

//
// constructor
//

indri::collection::RepositoryLoadThread::RepositoryLoadThread( indri::collection::Repository& repository, UINT64 memory ) :
  UtilityThread(),
  _repository( repository ),
  _memory( memory )
{
}

//
// initialize
//

UINT64 indri::collection::RepositoryLoadThread::initialize() {
  return FIVE_SECONDS;
}

//
// deinitialize
//

void indri::collection::RepositoryLoadThread::deinitialize() {
  // do nothing
}

//
// work
//

UINT64 indri::collection::RepositoryLoadThread::work() {
  _repository._incrementLoad();
  return 0;
}

//
// hasWork
//

bool indri::collection::RepositoryLoadThread::hasWork() {
  return false;
}



