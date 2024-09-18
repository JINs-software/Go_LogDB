package server

import (
	"context"

	api "github.com/JINs-software/GoLogDB/api/v1"
	"google.golang.org/grpc"
)

// 서비스는 특정한 로그 구현에 묶이지 않는 것이 좋음, 원할 때 필요한 로그 구현을 전달하는 것.
// 이를 위해선 로그 구현체가 아닌 로그 인터페이스에 의존하도록 해야한다.
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

// => 이 인터페이스를 통해 CommitLog 인터페이스를 만족하는 어떠한 로그 구현도 사용 가능

type Config struct {
	CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// 서비스를 인스턴스화, gRPC 서버를 생성, 서비스를 서버에 등록할 수 있게한다.
// 사용자는 연결 요청을 수락하는 리스너만 추가하면 된다.
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// log_grpc.pb.go 파일의 API를 구현
// -> Consume()과 Produce(), ProduceStream(), ConsumeStream() 핸들러를 구현

// [grpcServer.Produce]
// 클라이언트가 서버의 로그에 생산을 요청할 때 이를 처리
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// [grpcServer.Consume]
// 클라이언트가 서버의 로그의 소비를 요청할 때 이를 처리
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}
func (s *grpcServer) ProduceStream(
	stream api.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
