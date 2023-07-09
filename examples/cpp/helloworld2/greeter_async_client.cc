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

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld2::Greeter;
using helloworld2::HelloReply;
using helloworld2::HelloRequest;


class ClientAsyncCall
{
public:
	enum CallStatus {PROCESS, FINISH, DESTROY};

	CallStatus callStatus;

	std::string client_type;

	// Context for the client. It could be used to convey extra information to
	// the server and/or tweak certain RPC behaviors.
	ClientContext context;

	// Container for the data we expect from the server.
	HelloReply reply;

	// Storage for the status of the RPC upon completion.
	Status status;

	void PrintReply()
	{
		std::cout << "[" << client_type << "] reply message : " << reply.message() << std::endl; 
	}

public:
	explicit ClientAsyncCall(const char *type):callStatus(PROCESS), client_type(type) {}
	virtual ~ClientAsyncCall() {}

	virtual void Proceed(bool = true) = 0;
};

class RPCClientCall: public ClientAsyncCall
{
public:
	std::unique_ptr< ClientAsyncResponseReader<HelloReply> > responder;

	RPCClientCall(const HelloRequest& request, CompletionQueue& cq_, std::unique_ptr<Greeter::Stub>& stub_):
		ClientAsyncCall("Async RPC Client")
	{
	}

	virtual void Proceed(bool ok = true) override
	{
		if (callStatus == PROCESS) {
			if(status.ok())
				PrintReply();
			std::cout << "Delete RPC Call Client: " << this << std::endl;

			delete this;
		}
	}
};

class ReadStreamClientCall: public ClientAsyncCall
{
public:
	std::unique_ptr< ClientAsyncReader<HelloReply> > responder;

	ReadStreamClientCall(const HelloRequest& request, CompletionQueue& cq_, std::unique_ptr<Greeter::Stub>& stub_):
		ClientAsyncCall("Read Stream Client")
	{
	}

	virtual void Proceed(bool ok = true) override
	{
		if (callStatus == PROCESS) {
			if(!ok)
			{
				std::cout << "Going to Finish state " << this->client_type  << std::endl;
				responder->Finish(&status,  (void*)this);
				callStatus = FINISH;
				return;
			}
			responder->Read(&reply,  (void*)this);
			PrintReply();
		} else if(callStatus == FINISH)
		{
			std::cout << "Finish " << this->client_type  << std::endl;
			delete this;
		}
	}
};

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
    void SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    RPCClientCall* call = new  RPCClientCall(request, cq_, stub_);

    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responder =
        stub_->PrepareAsyncSayHello(&call->context, request, &cq_);

    // StartCall initiates the RPC call
    call->responder->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call
    // object.
    call->responder->Finish(&call->reply, &call->status, (void*)this);

    std::cout << "New " << call->client_type << std::endl;
    call->callStatus = ClientAsyncCall::PROCESS;

   }

    void SayHelloAndListenToGreeter(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    ReadStreamClientCall* call = new  ReadStreamClientCall(request, cq_, stub_);

    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->callStatus = ClientAsyncCall::PROCESS;
    call->responder =
        stub_->PrepareAsyncSayHelloStreamReply(&call->context, request, &cq_);
      //stub_->AsyncSayHelloStreamReply(&call->context, request, &cq_, (void*)this);

    // StartCall initiates the RPC call
      call->responder->StartCall(call);

    // server's response; "status" with the indication of whether the operation
    // was successful. 
    call->responder->Finish( &call->status, (void*)this);

    std::cout << "New " << call->client_type << std::endl;

   }

   void AsyncCompleteRpc(){
    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.

    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.

    while(cq_.Next(&got_tag, &ok))
    {
	    // The tag in this example is the memory location of the call object
	    ClientAsyncCall *call= static_cast<ClientAsyncCall *>(got_tag);
	    // Verify that the request was completed successfully. Note that "ok"
	    // corresponds solely to the request for updates introduced by Finish().
	    //GPR_ASSERT(ok);

	    call->Proceed(ok);
    }
    std::cout << "Complition queue shutdown" << std::endl;
  }

 private:
  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;

  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq_;
};

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  std::string target_str = absl::GetFlag(FLAGS_target);
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  GreeterClient greeter(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
  // Spawn reader thread that loops indefinitely
  std::thread thread_ = std::thread(&GreeterClient::AsyncCompleteRpc, &greeter);
  std::string user("world");
  greeter.SayHello(user);  // The actual RPC call!
  user = "sasha";
  greeter.SayHelloAndListenToGreeter(user);
  thread_.join();

  return 0;
}
