if(APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

# Add C library
add_library(driver SHARED driver.c)
set_target_properties(driver PROPERTIES PREFIX "" OUTPUT_NAME "driver")
target_link_libraries(driver ${MSGPUCK_LIBRARIES})

# Install module
install(FILES init.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/shard)
install(FILES connpool.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/shard)
install(TARGETS driver LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/shard)
