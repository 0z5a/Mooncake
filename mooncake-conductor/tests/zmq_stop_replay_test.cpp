#include <gtest/gtest.h>
#include <msgpack.hpp>
#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <array>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "conductor/zmq/zmq_client.h"

namespace {

class ReplayStopHandler : public conductor::zmq::EventHandler {
   public:
    std::string HandleBatch(const conductor::zmq::DecodedBatch&,
                            const conductor::zmq::MessageMetadata&) override {
        std::lock_guard lock(mu_);
        ++count_;
        cv_.notify_all();
        return "";
    }

    bool WaitForFirstBatch() {
        std::unique_lock lock(mu_);
        return cv_.wait_for(lock, std::chrono::seconds(2),
                            [this] { return count_ != 0; });
    }

    size_t Count() {
        std::lock_guard lock(mu_);
        return count_;
    }

   private:
    std::mutex mu_;
    std::condition_variable cv_;
    size_t count_ = 0;
};

TEST(ZMQClient, StopInterruptsPendingReplayWithoutDispatchingLiveBoundary) {
    ::zmq::context_t context(1);
    ::zmq::socket_t publisher(context, ::zmq::socket_type::xpub);
    ::zmq::socket_t replay(context, ::zmq::socket_type::router);
    for (auto* socket : {&publisher, &replay}) {
        socket->set(::zmq::sockopt::linger, 0);
        socket->set(::zmq::sockopt::rcvtimeo, 2000);
        socket->set(::zmq::sockopt::sndtimeo, 2000);
        socket->bind("tcp://127.0.0.1:*");
    }

    conductor::zmq::ZMQClientConfig config;
    config.endpoint = publisher.get(::zmq::sockopt::last_endpoint);
    config.replay_endpoint = replay.get(::zmq::sockopt::last_endpoint);
    config.poll_timeout = std::chrono::milliseconds(20);
    config.replay_timeout = std::chrono::seconds(3);
    auto handler = std::make_shared<ReplayStopHandler>();
    conductor::zmq::ZMQClient client(config, handler);
    ASSERT_EQ(client.Start(), "");

    // XPUB acknowledges subscription readiness; no slow-joiner sleep/retry.
    ::zmq::message_t subscription;
    ASSERT_TRUE(publisher.recv(subscription).has_value());
    ASSERT_EQ(subscription.size(), 1u);
    ASSERT_EQ(*static_cast<const unsigned char*>(subscription.data()), 1);

    msgpack::sbuffer payload;
    msgpack::packer<msgpack::sbuffer> packer(payload);
    packer.pack_array(3);
    packer.pack_double(1.25);
    packer.pack_array(0);
    packer.pack_nil();
    const std::string empty;
    auto publish = [&](unsigned char sequence) {
        std::array<unsigned char, 8> encoded{};
        encoded.back() = sequence;
        const std::array<::zmq::const_buffer, 3> frames = {
            ::zmq::buffer(empty), ::zmq::buffer(encoded),
            ::zmq::buffer(payload.data(), payload.size())};
        return ::zmq::send_multipart(publisher, frames).has_value();
    };
    ASSERT_TRUE(publish(10));
    ASSERT_TRUE(handler->WaitForFirstBatch());
    ASSERT_TRUE(publish(15));

    // Withhold the response only after observing a real gap-replay request.
    std::vector<::zmq::message_t> request;
    ASSERT_TRUE(
        ::zmq::recv_multipart(replay, std::back_inserter(request)).has_value());
    ASSERT_EQ(request.size(), 3u);
    ASSERT_TRUE(request[1].empty());
    const std::array<unsigned char, 8> expected_from = {0, 0, 0, 0,
                                                        0, 0, 0, 11};
    ASSERT_EQ(request[2].to_string(),
              std::string(reinterpret_cast<const char*>(expected_from.data()),
                          expected_from.size()));

    auto stopped = std::async(std::launch::async, [&] { client.Stop(); });
    const auto status = stopped.wait_for(std::chrono::milliseconds(500));
    if (status != std::future_status::ready) {
        // Release the old blocking receive so a regression fails promptly.
        std::array<unsigned char, 8> end_sequence;
        end_sequence.fill(0xff);
        const std::array<::zmq::const_buffer, 5> response = {
            ::zmq::buffer(request[0].data(), request[0].size()),
            ::zmq::buffer(empty), ::zmq::buffer(empty),
            ::zmq::buffer(end_sequence), ::zmq::buffer(empty)};
        EXPECT_TRUE(::zmq::send_multipart(replay, response).has_value());
    }
    stopped.get();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(handler->Count(), 1u);
    client.Stop();
}

}  // namespace
