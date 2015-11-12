cmake_minimum_required (VERSION 2.8)
project (hdf_wrapper_test)

include_directories("${PROJECT_BINARY_DIR}/../..")

find_package(HDF5)
find_package(Boost)

include_directories(${HDF5_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${HDF5_LIBRARY_DIRS})
link_directories(${Boost_LIBRARY_DIRS})

add_executable(hdf_wrapper_test test_hdf.cpp)
target_link_libraries(hdf_wrapper_test ${HDF5_LIBRARIES})

add_executable(should_not_compile1 should_not_compile1.cpp)
target_link_libraries(should_not_compile1 ${HDF5_LIBRARIES})