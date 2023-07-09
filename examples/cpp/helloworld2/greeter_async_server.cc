/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld2.grpc.pb.h"
#else
#include "helloworld2.grpc.pb.h"
#endif

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerAsyncWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using helloworld2::Greeter;
using helloworld2::HelloReply;
using helloworld2::HelloRequest;


class CallDataBase {
	public:
		// Take in the "service" instance (in this case representing an asynchronous
		// server) and the completion queue "cq" used for asynchronous communication
		// with the gRPC runtime.
		CallDataBase(Greeter::AsyncService* service, ServerCompletionQueue* cq)
			: service_(service), cq_(cq),status_(CREATE), prefix_("Hello") {
			}

		virtual ~CallDataBase()
		{
		}

		virtual void Proceed(bool = true) = 0;

	protected:
		// The means of communication with the gRPC runtime for an asynchronous
		// server.
		Greeter::AsyncService* service_;
		// The producer-consumer queue where for asynchronous server notifications.
		ServerCompletionQueue* cq_;
		// Context for the rpc, allowing to tweak aspects of it such as the use
		// of compression, authentication, as well as to send metadata back to the
		// client.
		ServerContext ctx_;

		// What we get from the client.
		HelloRequest request_;
		// What we send back to the client.
		HelloReply reply_;


		// Let's implement a tiny state machine with the following states.
		enum CallStatus { CREATE, PROCESS, FINISH };
		CallStatus status_;  // The current serving state.

		std::string prefix_;
};

class CallData: public CallDataBase
{

	// The means to get back to the client.
	ServerAsyncResponseWriter<HelloReply> responder_;

public:
	  CallData(Greeter::AsyncService* service, ServerCompletionQueue* cq):
		                  CallDataBase(service, cq), responder_(&ctx_)
		{
			// Invoke processing
			Proceed();
		}

	  void Proceed(bool = true) override {
		  if (status_ == CREATE) {
			  // Make this instance progress to the PROCESS state.
			  status_ = PROCESS;

			  // As part of the initial CREATE state, we *request* that the system
			  // start processing SayHello requests. In this request, "this" acts are
			  // the tag uniquely identifying the request (so that different CallData
			  // instances can serve different requests concurrently), in this case
			  // the memory address of this CallData instance.

			  std::cout << "New responder for 1:1 mode " << this << std::endl;

			  service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_,
					  this);
		  } else if (status_ == PROCESS) {
			  std::cout << "Create a new instance of responder to serve new client in 1:1 mode while this instance is busy with processing request message: " << request_.name()  << std::endl;
			  // Spawn a new CallData instance to serve new clients while we process
			  // the one for this CallData. The instance will deallocate itself as
			  // part of its FINISH state.
			  new CallData(service_, cq_);


			  // The actual processing.
			  reply_.set_message(prefix_ + request_.name());

			  // And we are done! Let the gRPC runtime know we've finished, using the
			  // memory address of this instance as the uniquely identifying tag for
			  // the event.
			  status_ = FINISH;
			  responder_.Finish(reply_, Status::OK, this);
		  } else {
			  GPR_ASSERT(status_ == FINISH);
			  std::cout << "Delete responder for 1:1 mode " << this << std::endl;
			  // Once in the FINISH state, deallocate ourselves (CallData).
			  delete this;
		  }
	  }
};

class CallDataWriter: public CallDataBase
{

	// The means to get back to the client.
	ServerAsyncWriter<HelloReply> responder_;

	int count_;
	bool new_responder_created_;

	std::vector<std::string> messages_;

	void InitMessages(const std::string& name) {

		std::stringstream ss;

		ss << this;

		messages_.push_back(prefix_ + name + "!"); 
		messages_.push_back("How are you doing?"); 
		messages_.push_back("How can I assist you today?"); 
		messages_.push_back("I'm a server ID " + ss.str()); 

		//std::copy(messages_.begin(), messages_.end(), std::ostream_iterator<std::string>(std::cout, " "));
		//std::cout << std::endl;
	}

public:
	  CallDataWriter(Greeter::AsyncService* service, ServerCompletionQueue* cq):
		                  CallDataBase(service, cq), responder_(&ctx_),count_(0),new_responder_created_(false)
		{
			// Invoke processing
			Proceed();
		}

	  void Proceed(bool = true) override {
		  if (status_ == CREATE) {
			  // Make this instance progress to the PROCESS state.
			  status_ = PROCESS;

			  // As part of the initial CREATE state, we *request* that the system
			  // start processing SayHello requests. In this request, "this" acts are
			  // the tag uniquely identifying the request (so that different CallData
			  // instances can serve different requests concurrently), in this case
			  // the memory address of this CallData instance.

			  std::cout << "New responder for 1:N mode " << this << std::endl;

			  service_->RequestSayHelloStreamReply(&ctx_, &request_, &responder_, cq_, cq_,
					  this);
		  } else if (status_ == PROCESS) {

			  if (!new_responder_created_) {
				  new_responder_created_ = true;
				  std::cout << "Create a new instance of responder to serve new client in 1:N mode while this instance is busy with processing request message: " << request_.name()  << std::endl;
				  // Spawn a new CallDataWriter instance to serve new clients while we process
				  // the one for this CallDataWriter. The instance will deallocate itself as
				  // part of its FINISH state.
				  new CallDataWriter(service_, cq_);
			  }

			  if (!count_)
				  InitMessages(request_.name());


			  // The actual processing.
			  
			  std::cout << "Responder 1:N [" << this << "] count = " << count_ << std::endl;
			  if (/*ctx_.IsCancelled() || */count_ >= messages_.size()) {
				  // And we are done! Let the gRPC runtime know we've finished, using the
				  // memory address of this instance as the uniquely identifying tag for
				  // the event.
				  std::cout << "Responder 1:N [" << this << "] Finishing " << std::endl;
				  status_ = FINISH;
				  responder_.Finish(Status::OK, (void*)this);
			  } else {
				  std::cout << "Responder 1:N [" << this << "] Writing " << messages_.at(count_)<< std::endl;
				  reply_.set_message(messages_.at(count_));
				  responder_.Write(reply_, (void*)this);
				  count_++;
			  }

		  } else {
			  GPR_ASSERT(status_ == FINISH);
			  std::cout << "Delete Responder 1:N [" << this << "]" << std::endl;
			  // Once in the FINISH state, deallocate ourselves (CallData).
			  delete this;
		  }
	  }
};

class ServerImpl final {
 public:
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run(uint16_t port) {
    std::string server_address = absl::StrFormat("0.0.0.0:%d", port);

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    HandleRpcs();
  }

 private:
  // Class encompasing the state and logic needed to serve a request.

  // This can be run in multiple threads if needed.
  void HandleRpcs() {
    // Spawn a new CallData instance to serve new clients.
    new CallData(&service_, cq_.get());
    new CallDataWriter(&service_, cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallDataBase*>(tag)->Proceed(ok);
    }
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  ServerImpl server;
  server.Run(absl::GetFlag(FLAGS_port));

  return 0;
}
