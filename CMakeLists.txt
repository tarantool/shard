cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
include(ExternalProject)

project(shard C)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Find Tarantool
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)

add_definitions("-D_GNU_SOURCE")
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Set CFLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

add_library(driver SHARED driver.c msgpuck.c hints.c)
set_target_properties(driver PROPERTIES PREFIX "" OUTPUT_NAME "driver")
add_custom_target(check
    COMMAND ${PROJECT_SOURCE_DIR}/test/mysql.test.lua)
