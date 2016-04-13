ADD_LIBRARY(index_engine SHARED index_engine.cc)
TARGET_LINK_LIBRARIES(index_engine mcd_util)
SET_TARGET_PROPERTIES(index_engine PROPERTIES PREFIX "")
