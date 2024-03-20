package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	sq "github.com/Masterminds/squirrel"
	desc "github.com/ebezgodov/auth/pkg/user_v1"
	"github.com/jackc/pgx/v4/pgxpool"

	"crypto/sha512"
	"encoding/hex"
)

const grpcPort = 50051

type server struct {
	desc.UnimplementedUserV1Server
	pool *pgxpool.Pool
}

func sha512_pwd(password string) string {
	// Вычисляем SHA512 хеш
	hasher := sha512.New()
	hasher.Write([]byte(password))
	hash := hasher.Sum(nil)

	// Преобразуем хеш в шестнадцатеричное представление
	hexhash := hex.EncodeToString(hash)

	// Возвращаем результат
	return hexhash
}

// Create ...
func (s *server) Create(ctx context.Context, req *desc.CreateRequest) (*desc.CreateResponse, error) {
	builderInsert := sq.Insert("auth").
		PlaceholderFormat(sq.Dollar).
		Columns("user_name", "email", "user_role", "user_password").
		Values(req.GetInfo().Name, req.Info.GetEmail(), req.GetInfo().Role, sha512_pwd(req.GetPassword())).
		Suffix("RETURNING id")

	query, args, err := builderInsert.ToSql()
	if err != nil {
		log.Fatalf("failed to build query: %v", err)
	}

	var userID int64
	err = s.pool.QueryRow(ctx, query, args...).Scan(&userID)
	if err != nil {
		log.Fatalf("failed to insert user: %v", err)
	}

	log.Printf("inserted user with id: %d", userID)

	return &desc.CreateResponse{
		Id: userID,
	}, nil
}

// Get ...
func (s *server) Get(ctx context.Context, req *desc.GetRequest) (*desc.GetResponse, error) {
	builderSelectOne := sq.Select("id", "user_name", "email", "user_role", "created_at", "updated_at").
		From("auth").
		PlaceholderFormat(sq.Dollar).
		Where(sq.Eq{"id": req.GetId()}).
		Limit(1)

	query, args, err := builderSelectOne.ToSql()
	if err != nil {
		log.Fatalf("failed to build query: %v", err)
	}

	var userId int64
	var userName, email string
	var userRole string
	var createdAt time.Time
	var updatedAt sql.NullTime

	err = s.pool.QueryRow(ctx, query, args...).Scan(&userId, &userName, &email, &userRole, &createdAt, &updatedAt)
	if err != nil {
		log.Fatalf("failed to select user: %v", err)
	}

	log.Printf("id: %d, user_name: %s, email: %s, user_role: %s, created_at: %v, updated_at: %v\n", userId, userName, email, userRole, createdAt, updatedAt)

	var updatedAtTime *timestamppb.Timestamp
	if updatedAt.Valid {
		updatedAtTime = timestamppb.New(updatedAt.Time)
	}

	return &desc.GetResponse{
		User: &desc.User{
			Id: userId,
			Info: &desc.UserInfo{
				Name:  userName,
				Email: email,
				Role:  desc.Role(desc.Role_value[userRole]),
			},
			CreatedAt: timestamppb.New(createdAt),
			UpdatedAt: updatedAtTime,
		},
	}, nil
}

// Update ...
func (s *server) Update(ctx context.Context, req *desc.UpdateRequest) (*emptypb.Empty, error) {
	builderUpdate := sq.Update("auth").
		PlaceholderFormat(sq.Dollar).
		Set("user_name", req.GetInfo().Name.Value).
		Set("email", req.GetInfo().Email.Value).
		Set("user_role", req.GetRole()).
		Set("updated_at", time.Now()).
		Where(sq.Eq{"id": req.GetId()})

	query, args, err := builderUpdate.ToSql()
	if err != nil {
		log.Fatalf("failed to build query: %v", err)
	}

	res, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		log.Fatalf("failed to update user: %v", err)
	}

	log.Printf("updated %d rows", res.RowsAffected())

	return new(emptypb.Empty), nil
}

// Delete ...
func (s *server) Delete(ctx context.Context, req *desc.DeleteRequest) (*emptypb.Empty, error) {
	builderDeleteOne := sq.Delete("auth").
		PlaceholderFormat(sq.Dollar).
		Where(sq.Eq{"id": req.GetId()})

	query, args, err := builderDeleteOne.ToSql()
	if err != nil {
		log.Fatalf("failed to build query: %v", err)
	}

	res, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		log.Fatalf("failed to delete user: %v", err)
	}

	log.Printf("deleted %d rows", res.RowsAffected())

	return new(emptypb.Empty), nil
}

// Main
func main() {
	ctx := context.Background()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Создаем пул соединений с базой данных
	pool, err := pgxpool.Connect(ctx, "host=pg-local port=5432 dbname=auth user=auth-user password=auth-password sslmode=disable")
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	s := grpc.NewServer()
	reflection.Register(s)
	desc.RegisterUserV1Server(s, &server{pool: pool})

	log.Printf("server listening at %v", lis.Addr())

	if err = s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
