#pragma once
#include <string>
#include <list>
#include <vector>
#include <map>
#include <queue>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <chrono>
#include <cassert>
#include <stdio.h>
#include <algorithm>  
#include <atomic>
#ifdef _MSC_VER
#include<windows.h> 
#endif

#include "macros.hpp"
#include "endec.hpp"
#include "raft_proto.hpp"
#include "utils.hpp"
#include "timer.hpp"
#include "functors.hpp"
#include "filelog.hpp"
#include "timer.hpp"
#include "committer.hpp"
#include "snapshot.hpp"
#include "metadata.hpp"
#include "raft_peer.hpp"
#include "raft_configuration.hpp"

