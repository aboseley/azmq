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

namespace {

std::string subj(const char *name) {
  return std::string("inproc://") + name;
}

std::array<boost::asio::const_buffer, 2> const snd_bufs = {{
                                                               boost::asio::buffer("A"),
                                                               boost::asio::buffer("B")
                                                           }};
}

TEST_CASE("REQ to REP combination, synchronous", "[socket]") {
  boost::asio::io_context ioc{};

  auto sb = azmq::rep_socket{ioc};
  sb.bind(subj(BOOST_CURRENT_FUNCTION));

  auto sc = azmq::req_socket{ioc};
  sc.connect(subj(BOOST_CURRENT_FUNCTION));

// request
  sc.send(snd_bufs);
  auto received_request = azmq::message_vector{};
  sb.receive_more(received_request, 0);
  REQUIRE(received_request.size() == 2);

// reply
  sb.send(snd_bufs);
  auto received_reply = azmq::message_vector{};
  sc.receive_more(received_reply, 0);
  REQUIRE(received_reply.size() == 2);
}

TEST_CASE("REQ to REP combination, asynchronous", "[socket]") {
  boost::asio::io_context ioc{};
  auto sb = azmq::rep_socket{ioc};
  auto sc = azmq::req_socket{ioc};
  bool caught_request = false;
  bool caught_reply = false;

  sb.bind(subj(BOOST_CURRENT_FUNCTION));
  sc.connect(subj(BOOST_CURRENT_FUNCTION));
  // catch the request
  sb.async_receive(
      [&](boost::system::error_code ec,
          azmq::message &first_frame,
          size_t bytes_transferred) {
        if (!ec) {
          auto message = azmq::message_vector{};
          sb.receive_more(message, ZMQ_DONTWAIT);
          REQUIRE(message.size() == 1);
          REQUIRE(first_frame == snd_bufs.at(0));
          REQUIRE(message.at(0) == snd_bufs.at(1));
          caught_request = true;

          // catch the reply
          sc.async_receive(
              [&](boost::system::error_code ec,
                  azmq::message &first_frame,
                  size_t bytes_transferred) {
                if (!ec) {
                  auto received_reply = azmq::message_vector{};
                  sc.receive_more(received_reply, ZMQ_DONTWAIT);
                  REQUIRE(received_reply.size() == 1);
                  caught_reply = true;
                }
              }
          );

          sb.send(snd_bufs);
        }
      }
  );
  sc.send(snd_bufs);
  ioc.run();
  REQUIRE(caught_request);
  REQUIRE(caught_reply);
}

TEST_CASE("DEALER to REP combination, synchronous", "[socket]") {
  boost::asio::io_context ioc{};
  auto const mq_name = subj(BOOST_CURRENT_FUNCTION);

  auto sb = azmq::rep_socket{ioc};
  sb.bind(mq_name);

  auto sc = azmq::dealer_socket{ioc};
  sc.connect(mq_name);

// dealer request
  sc.send("", ZMQ_SNDMORE); // emulate the envelope the REQ socket normal adds
  sc.send(snd_bufs);
  auto received_request = azmq::message_vector{};
  sb.receive_more(received_request, 0);
  REQUIRE(received_request.size() == 2);

// reply
  sb.send(snd_bufs);
  auto received_reply = azmq::message_vector{};
  sc.receive_more(received_reply, 0);
  REQUIRE(received_reply.size() == 3);
}

TEST_CASE("REQ to ROUTER combination, synchronous", "[socket]") {
  boost::asio::io_context ioc{};

  auto const mq_name = subj(BOOST_CURRENT_FUNCTION);

  auto sb = azmq::router_socket{ioc};
  sb.bind(mq_name);

  auto sc = azmq::req_socket{ioc};
  auto const requester_id = std::string{"123abc"};
  sc.set_option(azmq::socket::identity(requester_id));
  sc.connect(mq_name);

// Request
  sc.send(snd_bufs);
  auto received_request = azmq::message_vector{};
  sb.receive_more(received_request, 0);
  REQUIRE(received_request.size() == 4);
  REQUIRE(received_request.at(0).string() == requester_id);
  REQUIRE(received_request.at(1).string() == "");
  REQUIRE(received_request.at(2) == snd_bufs.at(0));
  REQUIRE(received_request.at(3) == snd_bufs.at(1));

// Reply
  auto const reply = azmq::message_vector{
      received_request.at(0),
      received_request.at(1),
      azmq::message("foo"),
      azmq::message("bar"),
  };
  sb.send(reply);

  auto received_reply = azmq::message_vector{};
  sc.receive_more(received_reply, 0);
  REQUIRE(received_reply.size() == 2);
  REQUIRE(received_reply.at(0).string() == std::string{"foo"});
  REQUIRE(received_reply.at(1).string() == std::string{"bar"});
}

TEST_CASE("REQ to ROUTER combination, asynchronous", "[socket]") {
  boost::asio::io_context ioc{};
  auto sb = azmq::router_socket{ioc};
  auto sc = azmq::req_socket{ioc};
  bool caught_request = false;
  bool caught_reply = false;

  sb.bind(subj(BOOST_CURRENT_FUNCTION));
  sc.connect(subj(BOOST_CURRENT_FUNCTION));
  // catch the request
  sb.async_receive(
      [&](boost::system::error_code ec,
          azmq::message &first_frame,
          size_t bytes_transferred) {
        REQUIRE(!ec);
        auto message = azmq::message_vector{};
        sb.receive_more(message, ZMQ_DONTWAIT);
        REQUIRE(message.size() == 3);
        REQUIRE(message.at(2) == snd_bufs.at(1));
        caught_request = true;

        // catch the reply
        sc.async_receive(
            [&sc,&caught_reply,&ioc](boost::system::error_code ec,
                azmq::message &first_frame,
                size_t bytes_transferred) {
              SCOPE_EXIT { ioc.stop(); };
              REQUIRE(!ec);
              auto received_reply = azmq::message_vector{};
              sc.receive_more(received_reply, ZMQ_DONTWAIT);
              REQUIRE(received_reply.size() == 1);
              caught_reply = true;
            }
        );

        sb.send(first_frame,ZMQ_SNDMORE);
        sb.send(message);
      }
  );
  sc.send(snd_bufs);
  ioc.run();
  REQUIRE(caught_request);
  REQUIRE(caught_reply);
}

TEST_CASE("DEALER to ROUTER combination, synchronous", "[socket]") {
  boost::asio::io_context ioc{};
  auto const mq_name = subj(BOOST_CURRENT_FUNCTION);

  auto sb = azmq::router_socket{ioc};
  sb.bind(mq_name);

  auto sc = azmq::dealer_socket{ioc};
  auto const requester_id = std::string{"async_requester"};
  sc.set_option(azmq::socket::identity(requester_id));
  sc.connect(mq_name);

// Request
  sc.send("", ZMQ_SNDMORE); // emulate the envelope the REQ socket normal adds
  sc.send(snd_bufs);

  auto received_request = azmq::message_vector{};
  sb.receive_more(received_request, 0);
  REQUIRE(received_request.size() == 4);
  REQUIRE(received_request.at(0).string() == requester_id);
  REQUIRE(received_request.at(1).string() == "");
  REQUIRE(received_request.at(2) == snd_bufs.at(0));
  REQUIRE(received_request.at(3) == snd_bufs.at(1));

// Reply
  auto const reply = azmq::message_vector{
      received_request.at(0),
      received_request.at(1),
      azmq::message("foo"),
      azmq::message("bar"),
  };
  sb.send(reply);

  auto received_reply = azmq::message_vector{};
  sc.receive_more(received_reply, 0);
  REQUIRE(received_reply.size() == 3);
  REQUIRE(received_reply.at(0).string() == std::string{""});
  REQUIRE(received_reply.at(1).string() == std::string{"foo"});
  REQUIRE(received_reply.at(2).string() == std::string{"bar"});
}


