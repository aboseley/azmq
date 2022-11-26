cmake_minimum_required(VERSION 3.16...3.25)

project(test_azmq LANGUAGES CXX)

if (TEST_INSTALLED_VERSION)
    find_package(azmq 1.1 REQUIRED)
    enable_testing()
endif ()

macro (add_catch_test name)
    if (TEST_REPORT_FORMAT)
        add_test(NAME ${name} COMMAND ${name} -r ${TEST_REPORT_FORMAT} -o "${name}.test_out.xml")
    else ()
        add_test(NAME ${name} COMMAND ${name})
    endif ()
endmacro ()

add_subdirectory(message)
add_subdirectory(context_ops)
add_subdirectory(socket_ops)
add_subdirectory(socket)
add_subdirectory(signal)
add_subdirectory(actor)
