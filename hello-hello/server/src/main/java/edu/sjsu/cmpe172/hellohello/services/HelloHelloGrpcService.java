package edu.sjsu.cmpe172.hellohello.services;

import edu.sjsu.cmpe172.hellohello.HelloHello;
import edu.sjsu.cmpe172.hellohello.PostReplicaServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class HelloHelloGrpcService extends PostReplicaServiceGrpc.PostReplicaServiceImplBase {
    @Override
    public void newPost(HelloHello.NewPostRequest request, StreamObserver<HelloHello.NewPostReply> responseObserver) {
        System.out.println("Received newPost request: " + request);
        responseObserver.onNext(HelloHello.NewPostReply.newBuilder().setStatus(HelloHello.AddPostStatus.ADD_FAILED).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getLastTxn(HelloHello.GetLastTxnRequest request, StreamObserver<HelloHello.GetLastTxnReply> responseObserver) {
        responseObserver.onNext(HelloHello.GetLastTxnReply.newBuilder().setLastTxn(-1).build());
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<HelloHello.SyncWithLeaderRequest> syncWithLeader(StreamObserver<HelloHello.SyncWithLeaderReply> responseObserver) {
        return super.syncWithLeader(responseObserver);
    }
}
