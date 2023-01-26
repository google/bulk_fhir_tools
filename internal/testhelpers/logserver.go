// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testhelpers

import (
	"context"
	"net"
	"sync"
	"time"

	log "github.com/golang/glog"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	logpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/grpc"
)

// GCPLogServer creates an in-memory fake server implementing the GCP logging
// service and starts it. Call close on the server when done. GetLogsAfterClose
// can be used to verify logs were written.
func GCPLogServer() (*GRPCServer, *LoggingHandler, error) {
	srv, err := newServer()
	if err != nil {
		return nil, nil, err
	}
	loghandler := &LoggingHandler{
		logs: make(map[string][]*logpb.LogEntry),
	}
	logpb.RegisterLoggingServiceV2Server(srv.gsrv, loghandler)
	srv.start()
	return srv, loghandler, nil
}

// GRPCServer is an in-process gRPC server, listening on the local loopback
// interface. GRPCServers are for testing only and are not intended to be used in
// production code.
type GRPCServer struct {
	addr string
	l    net.Listener
	gsrv *grpc.Server
}

// newServer creates a new Server listening for gRPC connections at the address
// named by the Addr field, without TLS.
func newServer() (*GRPCServer, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	s := &GRPCServer{
		addr: l.Addr().String(),
		l:    l,
		gsrv: grpc.NewServer(),
	}
	return s, nil
}

// start causes the server to start accepting incoming connections.
// Call Start after registering handlers.
func (s *GRPCServer) start() {
	go func() {
		if err := s.gsrv.Serve(s.l); err != nil {
			log.Infof("GRPCServer.start error: %v", err)
		}
	}()
}

// Close shuts down the server.
func (s *GRPCServer) Close() {
	s.gsrv.GracefulStop()
}

// Addr returns the address of the server.
func (s *GRPCServer) Addr() string {
	return s.addr
}

// LoggingHandler implements the log rpc server and holds an in memory map of logs.
type LoggingHandler struct {
	logpb.LoggingServiceV2Server

	mu   sync.Mutex
	logs map[string][]*logpb.LogEntry // indexed by log name
}

// WriteLogEntries usually writes log entries to Cloud Logging. In this fake it
// saves the logs in an in-memory map. This function must be exported for the
// grpc server to work but should not be called directly in tests.
func (h *LoggingHandler) WriteLogEntries(_ context.Context, req *logpb.WriteLogEntriesRequest) (*logpb.WriteLogEntriesResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, e := range req.Entries {
		// If these fields do not exist in the entries, the server uses the request
		// to fill them out.
		if e.Timestamp == nil {
			e.Timestamp = &tspb.Timestamp{Seconds: time.Now().Unix(), Nanos: 0}
		}
		if e.LogName == "" {
			e.LogName = req.LogName
		}
		if e.Resource == nil {
			e.Resource = req.Resource
		}
		for k, v := range req.Labels {
			if _, ok := e.Labels[k]; !ok {
				e.Labels[k] = v
			}
		}
		h.logs[e.LogName] = append(h.logs[e.LogName], e)
	}
	return &logpb.WriteLogEntriesResponse{}, nil
}

// GetLogsAfterClose returns the in-memory map of written logs. It should be
// called after the Log Client is closed or flushed.
func (h *LoggingHandler) GetLogsAfterClose() map[string][]*logpb.LogEntry {
	return h.logs
}
