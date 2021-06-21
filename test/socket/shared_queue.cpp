/*
    Copyright (c) 2013-2014 Contributors as noted in the AUTHORS file

    This file is part of azmq

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
*/

#include <azmq/socket.hpp>
#include <boost/asio/buffer.hpp>
#include <catch2/catch.hpp>
#include <array>
#include <string>
#include <thread>
#include <boost/asio/steady_timer.hpp>
#include <boost/atomic/atomic_flag.hpp>
#include <iostream>

namespace {

std::array<boost::asio::const_buffer, 2> const snd_bufs = {{
                                                               boost::asio::buffer("A"),
                                                               boost::asio::buffer("B")
                                                           }};

auto CopyMore(azmq::message const &source_first_frame,
          azmq::socket &source,
          azmq::socket &destination
          ) ->void {
  auto more = source_first_frame.more();
  destination.send(source_first_frame, more ? ZMQ_SNDMORE : 0);
  while (more) {
    auto next_msg = azmq::message{};
    source.receive(next_msg);
    more = next_msg.more();
    destination.send(next_msg, more ? ZMQ_SNDMORE : 0);
  }
};

auto AsyncProxy( azmq::socket &source, azmq::socket &destination ) -> void{
  source.async_receive([&](boost::system::error_code ec,
                           azmq::message &first_frame,
                           size_t /*bytes_transferred*/) {
    if (!ec) {
      CopyMore(first_frame, source, destination);
      AsyncProxy( source, destination);
    }
  });
};

}

TEST_CASE("Shared queue", "[socket]") {

  auto rrclient = [](std::string const &queue_name) {
    boost::asio::io_context ioc{};
    auto requester = azmq::req_socket{ioc};
    requester.connect(queue_name);
    for (int request_num = 0; request_num < 10; request_num++) {
      requester.send(boost::asio::buffer("Do this thing"));
      azmq::message_vector reply{};
      requester.receive_more(reply, 0);
    }
    requester.send(boost::asio::buffer("All work done"));
  };

  auto rrworker = [](std::string const &queue_name) {
    boost::asio::io_context ioc{};
    auto rep = azmq::rep_socket{ioc};
    rep.connect(queue_name);
    while (1) {
      azmq::message_vector request{};
      rep.receive_more(request, 0);
      if (request.size() == 1 && request.at(0) == boost::asio::buffer("All work done")) {
        break;
      } else {
        auto timer = boost::asio::steady_timer(ioc);
        timer.expires_after(std::chrono::milliseconds(1));
        timer.wait();
        rep.send("I've done what you asked");
      }
    }
  };

  auto rrbroker = [](std::string const& front_queue_name, std::string const & back_queue_name,
                     boost::asio::io_context &ioc) {

    auto frontend = azmq::router_socket{ioc};
    frontend.bind(front_queue_name);

    auto backend = azmq::dealer_socket{ioc};
    backend.bind(back_queue_name);

    AsyncProxy( frontend, backend );
    AsyncProxy( backend, frontend);

    ioc.run();
  };

  auto const backend_queue_name = std::string("inproc://backend_");
  auto const frontend_queue_name = std::string("inproc://frontend_");

  boost::asio::io_context broker_ioc{};
  auto broker = std::thread([&] { rrbroker(frontend_queue_name, backend_queue_name,broker_ioc); });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto worker1 = std::thread([&] { rrworker(backend_queue_name); });
  auto worker2 = std::thread([&] { rrworker(backend_queue_name); });
  auto client1 = std::thread([&] { rrclient(frontend_queue_name); });
  auto client2 = std::thread([&] { rrclient(frontend_queue_name); });

  client1.join();
  client2.join();
  worker1.join();
  worker2.join();

  broker_ioc.stop();
  broker.join();

}
