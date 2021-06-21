project(test_socket)

add_executable(${PROJECT_NAME} main.cpp request_reply_combinations.cpp shared_queue.cpp forwarder_device.cpp)

target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES}
                                      ${ZeroMQ_LIBRARIES}
                                      ${CMAKE_THREAD_LIBS_INIT})

add_catch_test(${PROJECT_NAME})
