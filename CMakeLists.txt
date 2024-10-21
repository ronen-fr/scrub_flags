cmake_minimum_required(VERSION 3.22)

# price of listing the scrubber flags

set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_EXTENSIONS OFF)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


project(flagsls CXX)

#dd_compile_options(-std=c++20)

set(CMAKE_CXX_FLAGS         "${CMAKE_CXX_FLAGS} -O3 -Wall -fomit-frame-pointer -I/home/rfriedma/apps/fmt/include -L/home/rfriedma/apps/fmt /home/rfriedma/apps/fmt/libfmt.a")
find_package(benchmark REQUIRED)



set(SOURCE main.cc)


add_executable(${PROJECT_NAME} ${SOURCE})

target_link_libraries(flagsls benchmark::benchmark /home/rfriedma/apps/fmt/libfmt.a)


