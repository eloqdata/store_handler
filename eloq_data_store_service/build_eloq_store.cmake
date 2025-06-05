cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(ELOQ_STORE_SOURCE_DIR ${ELOQSTORE_PARENT_DIR}/eloq_store)

find_package(Threads REQUIRED)
find_package(glog REQUIRED)

find_package(Boost REQUIRED COMPONENTS context)
if(Boost_FOUND)
    message(STATUS "Boost found at: ${Boost_INCLUDE_DIRS}")
else()
    message(FATAL_ERROR "Boost.Context not found!")
endif()

find_path(URING_INCLUDE_PATH NAMES liburing.h)
find_library(URING_LIB NAMES uring)
if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
    message(FATAL_ERROR "Fail to find liburing")
endif()

find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${ELOQ_STORE_SOURCE_DIR}/.git")
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        # Update submodules as needed
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${ELOQ_STORE_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

if(NOT EXISTS "${ELOQ_STORE_SOURCE_DIR}/concurrentqueue/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif()
add_subdirectory(${ELOQ_STORE_SOURCE_DIR}/concurrentqueue)

add_executable(io_uring_test ${ELOQ_STORE_SOURCE_DIR}/io_uring_test.cpp)
target_link_libraries(io_uring_test ${URING_LIB})

add_executable(uring_register_test ${ELOQ_STORE_SOURCE_DIR}/uring_register_test.cpp)
target_link_libraries(uring_register_test ${URING_LIB})

option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
endif()

SET(ELOQ_STORE_INCLUDE
    ${ELOQ_STORE_SOURCE_DIR}
    ${URING_INCLUDE_PATH}
    ${Boost_INCLUDE_DIRS}
    )

set(ELOQ_STORE_SOURCES
    ${ELOQ_STORE_SOURCE_DIR}/coding.cpp
    ${ELOQ_STORE_SOURCE_DIR}/data_page_builder.cpp
    ${ELOQ_STORE_SOURCE_DIR}/comparator.cpp
    ${ELOQ_STORE_SOURCE_DIR}/index_page_builder.cpp
    ${ELOQ_STORE_SOURCE_DIR}/mem_index_page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/index_page_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/write_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/read_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/scan_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/batch_write_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/truncate_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/compact_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/archive_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/write_tree_stack.cpp
    ${ELOQ_STORE_SOURCE_DIR}/async_io_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/data_page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/page_mapper.cpp
    ${ELOQ_STORE_SOURCE_DIR}/task_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/eloq_store.cpp
    ${ELOQ_STORE_SOURCE_DIR}/shard.cpp
    ${ELOQ_STORE_SOURCE_DIR}/root_meta.cpp
    ${ELOQ_STORE_SOURCE_DIR}/replayer.cpp
    ${ELOQ_STORE_SOURCE_DIR}/xxhash.c
    ${ELOQ_STORE_SOURCE_DIR}/kill_point.cpp
    ${ELOQ_STORE_SOURCE_DIR}/file_gc.cpp
    ${ELOQ_STORE_SOURCE_DIR}/archive_crond.cpp)

add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES})

target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(eloqstore PRIVATE ${URING_LIB} Boost::context glog::glog)

add_executable(io_test ${ELOQ_STORE_SOURCE_DIR}/io_test.cpp)
target_link_libraries(io_test ${URING_LIB})

add_executable(queue_sync_test ${ELOQ_STORE_SOURCE_DIR}/queue_sync_test.cpp)

add_executable(cache_q_test ${ELOQ_STORE_SOURCE_DIR}/cache_q_test.cpp)
# target_compile_options(cache_q_test PRIVATE -fsanitize=address)
# target_link_options(cache_q_test PRIVATE -fsanitize=address)
target_link_libraries (cache_q_test glog::glog)

# add_executable(proxy proxy.cc helpers.cc)
# target_link_libraries(proxy ${URING_LIB})

#if (WITH_UNIT_TESTS)
#  add_subdirectory(${ELOQ_STORE_SOURCE_DIR}/tests)
#endif()
