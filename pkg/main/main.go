package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"math"
	"net"
	"net/netip"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/datawire/go-fuseftp/pkg/fs"
	"github.com/datawire/go-fuseftp/rpc"
)

//go:embed version.txt
var version string

type mount struct {
	mountPoint string
	cancel     context.CancelFunc
	ftpClient  fs.FTPClient
}

// service represents the state of the Telepresence Daemon
type service struct {
	rpc.UnsafeFuseFTPServer
	sync.Mutex
	nextID int32
	mounts map[int32]*mount
	ctx    context.Context
}

func (s *service) Version(context.Context, *emptypb.Empty) (*rpc.VersionInfo, error) {
	return &rpc.VersionInfo{Semver: version}, nil
}

func addrPort(ap *rpc.AddressAndPort) (netip.AddrPort, error) {
	ip, ok := netip.AddrFromSlice(ap.Ip)
	if !ok || ap.Port < 1 || ap.Port > math.MaxUint16 {
		return netip.AddrPort{}, status.Errorf(codes.InvalidArgument, "invalid address")
	}
	return netip.AddrPortFrom(ip, uint16(ap.Port)), nil
}

func (s *service) Mount(_ context.Context, rq *rpc.MountRequest) (*rpc.MountIdentifier, error) {
	s.Lock()
	defer s.Unlock()

	for _, m := range s.mounts {
		if m.mountPoint == rq.MountPoint {
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("directory %q is already mounted", rq.MountPoint))
		}
	}

	ap, err := addrPort(rq.FtpServer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(s.ctx)
	fi, err := fs.NewFTPClient(ctx, ap, rq.Directory, rq.ReadTimeout.AsDuration())
	if err != nil {
		cancel()
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	host := fs.NewHost(fi, rq.MountPoint)
	host.Start(ctx)

	id := s.nextID
	s.mounts[id] = &mount{
		mountPoint: rq.MountPoint,
		ftpClient:  fi,
		cancel: func() {
			s.Lock()
			delete(s.mounts, id)
			s.Unlock()
			host.Stop()
			cancel()
		},
	}
	s.nextID++
	return &rpc.MountIdentifier{Id: id}, nil
}

func (s *service) Unmount(_ context.Context, rq *rpc.MountIdentifier) (*emptypb.Empty, error) {
	s.Lock()
	m, ok := s.mounts[rq.Id]
	s.Unlock()
	if ok {
		m.cancel()
	}
	return &emptypb.Empty{}, nil
}

func (s *service) SetFtpServer(_ context.Context, rq *rpc.SetFtpServerRequest) (*emptypb.Empty, error) {
	id := rq.Id.Id
	s.Lock()
	m, ok := s.mounts[id]
	s.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "found no mount with id %d", id)
	}
	ap, err := addrPort(rq.FtpServer)
	if err != nil {
		return nil, err
	}
	if err = m.ftpClient.SetAddress(s.ctx, ap); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, err
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <path to unix socket>\n", os.Args[0])
	}
	// Yes, this works on Windows too
	sa, err := net.ResolveUnixAddr("unix", os.Args[1])
	if err != nil {
		log.Fatalf("Failed to resolve unix socket address: %v\n", err)
	}
	ss, err := net.ListenUnix("unix", sa)
	if err != nil {
		log.Fatalf("Listen to unix socket failed: %v\n", err)
	}
	gs := grpc.NewServer()
	rpc.RegisterFuseFTPServer(gs, &service{
		mounts: make(map[int32]*mount),
		ctx:    context.Background()})
	if err := gs.Serve(ss); err != nil {
		log.Fatal(err.Error())
	}
}
