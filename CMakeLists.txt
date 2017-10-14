cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
include(ExternalProject)

project(shard C)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Find Tarantool
set(TARANTOOL_FIND_REQUIRED ON)
set(CMAKE_INSTALL_DATADIR "" )
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Find MsgPuck llibrary
set(MsgPuck_FIND_REQUIRED ON)
find_package(MsgPuck)
include_directories(${MSGPUCK_INCLUDE_DIRS})

add_definitions("-D_GNU_SOURCE")

# Set CFLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

if(APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

add_library(driver SHARED driver.c)
set_target_properties(driver PROPERTIES PREFIX "" OUTPUT_NAME "driver")
target_link_libraries(driver ${MSGPUCK_LIBRARIES})

install(TARGETS driver LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/shard)
install(FILES shard.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR})
