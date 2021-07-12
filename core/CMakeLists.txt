cmake_minimum_required(VERSION 3.9)
project(oneseismic LANGUAGES CXX)

include(CheckIncludeFile)
include(CTest)
include(GNUInstallDirs)
include(TestBigEndian)

option(BUILD_PYTHON  "Build Python library"                ON)

if(SKBUILD)
  # Scikit-Build does not add your site-packages to the search path automatically,
  # so we need to add it _or_ the pybind11 specific directory here.
  execute_process(
    COMMAND
      "${PYTHON_EXECUTABLE}" -c
      "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE _tmp_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ECHO STDOUT)
  list(APPEND CMAKE_PREFIX_PATH "${_tmp_dir}")
endif()
# Now we can find pybind11
find_package(pybind11 CONFIG REQUIRED)
#pybind11_add_module(_core MODULE src/assembler.cpp)
#target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})


add_library(json INTERFACE)
target_include_directories(json INTERFACE external/nlohmann)

find_package(fmt        REQUIRED)
find_package(spdlog     REQUIRED)

if (NOT MSVC)
    # assuming gcc-style options
    # add warnings in debug mode
    list(APPEND warnings-c++ -Wall
                             -Wextra
                             -pedantic
                             -Wformat-nonliteral
                             -Wcast-align
                             -Wpointer-arith
                             -Wmissing-declarations
                             -Wcast-qual
                             -Wwrite-strings
                             -Wchar-subscripts
                             -Wredundant-decls
    )
endif ()

set(CMAKE_CXX_STANDARD 14)

add_library(oneseismic
    src/base64.cpp
    src/geometry.cpp
    src/messages.cpp
    src/plan.cpp
    src/process.cpp
)
add_library(oneseismic::oneseismic ALIAS oneseismic)
target_include_directories(oneseismic
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        ${CURL_INCLUDE_DIRS}
    PRIVATE
        include
        external/nlohmann
)
target_link_libraries(oneseismic
    PUBLIC
        fmt::fmt
)

#install(TARGETS _core DESTINATION .)

install(
    TARGETS
        oneseismic
    EXPORT
        oneseismic-export
    ARCHIVE DESTINATION
        ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION
        ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION
        ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION include)
install(
    EXPORT
        oneseismic-export
    DESTINATION
        ${CMAKE_INSTALL_DATAROOTDIR}/oneseismic/cmake
    FILE
        oneseismic-config.cmake
    NAMESPACE
        oneseismic::
)
export(
    TARGETS
        oneseismic
    FILE
        oneseismic-config.cmake
    NAMESPACE
        oneseismic::
)
set(ONESEISMIC_LIB_CMAKECONFIG_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE PATH "")

if (NOT BUILD_TESTING)
    return()
endif ()

add_subdirectory(external/catch2)

add_executable(tests
    tests/testsuite.cpp
    tests/geometry.cpp
    tests/messages.cpp
    tests/process.cpp
)
target_link_libraries(tests
    PRIVATE
        catch2
        oneseismic::oneseismic
        fmt::fmt
        json
)
add_test(NAME unit-tests COMMAND tests)
