/*
    Copyright (c) 2013-2014 Contributors as noted in the AUTHORS file

    This file is part of azmq

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
*/

#include <azmq/socket.hpp>
#include <catch2/catch.hpp>
#include <thread>

namespace {

auto CopyMore(azmq::message const &source_first_frame,
              azmq::socket &source,
              azmq::socket &destination
) -> void {
  auto more = source_first_frame.more();
  destination.send(source_first_frame, more ? ZMQ_SNDMORE : 0);
  while (more) {
    auto next_msg = azmq::message{};
    source.receive(next_msg);
    more = next_msg.more();
    destination.send(next_msg, more ? ZMQ_SNDMORE : 0);
  }
};

auto AsyncForward(azmq::socket &source, azmq::socket &destination) -> void {
  source.async_receive([&](boost::system::error_code ec,
                           azmq::message &first_frame,
                           size_t /*bytes_transferred*/) {
    if (!ec) {
      CopyMore(first_frame, source, destination);
      AsyncForward(source, destination);
    }
  });
};

}

TEST_CASE("Forwarding Device", "[socket]") {
  auto subscriber = [](std::string const &queue_name) {
    boost::asio::io_context ioc{};
    auto sub = azmq::sub_socket{ioc};
    sub.connect(queue_name);
    sub.set_option(azmq::socket::subscribe(""));

    for (int i = 0; i < 20; i++) {
      auto event = azmq::message_vector{};
      sub.receive_more(event, 0);
      CHECK(event.size() == 1);
    }

  };

  auto publisher = [](std::string const &queue_name) {
    boost::asio::io_context ioc{};
    auto pub = azmq::pub_socket{ioc};
    pub.connect(queue_name);
    // give the subscriber a moment to subscribe
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // then start publishing
    for (int i = 0; i < 10; i++) {
      auto const msg = std::to_string(i);
      pub.send(boost::asio::buffer(msg));
    }
  };

  auto forwarding_device = [](std::string const &front_queue_name,
                              std::string const &back_queue_name,
                              boost::asio::io_context &ioc) {

    auto frontend = azmq::xsub_socket{ioc};
    frontend.bind(front_queue_name);

    auto backend = azmq::xpub_socket{ioc};
    backend.bind(back_queue_name);

    // Forward data to subscribers
    AsyncForward(frontend, backend);

    //forward subscriptions to the publishers
    AsyncForward(backend, frontend);

    ioc.run();
  };

  // data flow from the front->back
  auto const frontend_queue_name = std::string("inproc://x_pub");
  auto const backend_queue_name = std::string("inproc://x_sub");

  boost::asio::io_context ioc{};
  std::thread forwarder([&] { forwarding_device(frontend_queue_name, backend_queue_name, ioc); });
  std::thread p1([&] { publisher(frontend_queue_name); });
  std::thread p2([&] { publisher(frontend_queue_name); });
  std::thread s1([&] { subscriber(backend_queue_name); });
  std::thread s2([&] { subscriber(backend_queue_name); });

  p1.join();
  p2.join();
  s1.join();
  s2.join();
  ioc.stop();
  forwarder.join();;
}
