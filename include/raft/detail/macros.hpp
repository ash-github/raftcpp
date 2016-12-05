#pragma once
#define check_apply(Func) \
if(!(Func)){\
	std::cout << "FILE:" <<__FILE__ <<" LINE:"<< __LINE__ << "  FUNCTION:"<<__FUNCTION__<<std::endl;\
	return false;\
}