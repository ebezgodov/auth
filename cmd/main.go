package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/brianvoe/gofakeit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	desc "github.com/ebezgodov/auth/pkg/user_v1"
)

const grpcPort = 50051

type server struct {
	desc.UnimplementedUserV1Server
}

// Create ...
func (s *server) Create(_ context.Context, req *desc.CreateRequest) (*desc.CreateResponse, error) {

	log.Printf("User name: %s", req.GetInfo().Name)

	return &desc.CreateResponse{
		Id: gofakeit.Int64(),
	}, nil
}

// Get ...
func (s *server) Get(_ context.Context, req *desc.GetRequest) (*desc.GetResponse, error) {

	log.Printf("User id: %d", req.GetId())

	return &desc.GetResponse{
		User: &desc.User{
			Id: req.GetId(),
			Info: &desc.UserInfo{
				Name:  gofakeit.Name(),
				Email: gofakeit.Email(),
				Role:  desc.Role_user,
			},
			Password:  "***",
			CreatedAt: timestamppb.New(gofakeit.Date()),
			UpdatedAt: timestamppb.New(gofakeit.Date()),
		},
	}, nil
}

// Update ...
func (s *server) Update(_ context.Context, req *desc.UpdateRequest) (*emptypb.Empty, error) {

	log.Printf("User id: %d", req.GetId())

	out := new(emptypb.Empty)
	return out, nil
}

// Delete ...
func (s *server) Delete(_ context.Context, req *desc.DeleteRequest) (*emptypb.Empty, error) {

	log.Printf("User id: %d", req.GetId())

	out := new(emptypb.Empty)
	return out, nil
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	reflection.Register(s)
	desc.RegisterUserV1Server(s, &server{})

	log.Printf("server listening at %v", lis.Addr())

	if err = s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
