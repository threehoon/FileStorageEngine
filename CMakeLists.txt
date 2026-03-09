cmake_minimum_required(VERSION 3.10.0)
project(TFS_FileStorageEngine VERSION 0.1.0 LANGUAGES C CXX)

# 定义了一个SRC_LIST变量，包含了该目录下所有的源文件
aux_source_directory( ./src  SRC_LIST )

# 头文件搜索目录
include_directories(${PROJECT_SOURCE_DIR}/include )

# 设置可执行文件输出目录
set( CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin )

# add_executable(TFS_FileStorageEngine ${SRC_LIST} )

# example 下的每个 cpp（ 每一个文件都是一个脚本 ） 都生成一个可执行文件 
file(GLOB EXAMPLE_SOURCES "${PROJECT_SOURCE_DIR}/example/*.cpp")

foreach(src ${EXAMPLE_SOURCES})
    get_filename_component(exe_name ${src} NAME_WE )    # 获取文件名（不带后缀）
    add_executable(${exe_name} ${src} ${SRC_LIST})     # 链接 src 里的源码
endforeach()

include(CTest)

enable_testing( )

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
