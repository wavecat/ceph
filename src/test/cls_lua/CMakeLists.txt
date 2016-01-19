add_executable(ceph_test_cls_lua test_cls_lua.cc)

set_target_properties(ceph_test_cls_lua
    PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

target_include_directories(ceph_test_cls_lua
    PUBLIC ${LUA_INCLUDE_DIR})

target_link_libraries(ceph_test_cls_lua
    ${LUA_LIBRARIES} cls_lua_client librados global
    ${UNITTEST_LIBS} ${CMAKE_DL_LIBS} ${EXTRALIBS}
    radostest)

install(TARGETS ceph_test_cls_lua
    DESTINATION ${CMAKE_INSTALL_BINDIR})
