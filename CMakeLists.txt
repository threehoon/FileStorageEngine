cmake_minimum_required(VERSION 3.10.0)
project(TFS_FileStorageEngine VERSION 0.1.0 LANGUAGES C CXX)

# 定义了一个SRC_LIST变量，包含了该目录下所有的源文件
aux_source_directory( ./src  SRC_LIST )

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(TFS_FileStorageEngine ${SRC_LIST} )

include(CTest)
enable_testing( )

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)
