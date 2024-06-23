// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	context "context"
	"math/rand"
	"os"
	"time"

	"github.com/yeastengine/config5g/logger"
	protos "github.com/yeastengine/config5g/proto/sdcoreConfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

var (
	selfRestartCounter      uint32
	configPodRestartCounter uint32 = 0
)

func init() {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	selfRestartCounter = r1.Uint32()
}

type PlmnId struct {
	MCC string
	MNC string
}

type Nssai struct {
	sst string
	sd  string
}

type ConfigClient struct {
	Client            protos.ConfigServiceClient
	Conn              *grpc.ClientConn
	Channel           chan *protos.NetworkSliceResponse
	Host              string
	Version           string
	MetadataRequested bool
}

type ConfClient interface {
	// channel is created on which subscription is done.
	// On Receiving Configuration from ConfigServer, this api publishes
	// on created channel and returns the channel
	PublishOnConfigChange(bool) chan *protos.NetworkSliceResponse

	// returns grpc connection object
	GetConfigClientConn() *grpc.ClientConn

	// Client Subscribing channel to ConfigPod to receive configuration
	subscribeToConfigPod(commChan chan *protos.NetworkSliceResponse)
}

// This API is added to control metadata from NF Clients
func ConnectToConfigServer(host string) ConfClient {
	confClient := CreateChannel(host, 10000)
	if confClient == nil {
		logger.GrpcLog.Errorln("create grpc channel to config pod failed")
		return nil
	}
	return confClient
}

func (confClient *ConfigClient) PublishOnConfigChange(mdataFlag bool) chan *protos.NetworkSliceResponse {
	confClient.MetadataRequested = mdataFlag
	commChan := make(chan *protos.NetworkSliceResponse)
	confClient.Channel = commChan
	go confClient.subscribeToConfigPod(commChan)
	return commChan
}

// pass structr which has configChangeUpdate interface
func ConfigWatcher(webuiUri string) chan *protos.NetworkSliceResponse {
	// var confClient *gClient.ConfigClient
	// TODO: use port from configmap.
	confClient := CreateChannel(webuiUri, 10000)
	if confClient == nil {
		logger.GrpcLog.Errorf("create grpc channel to config pod failed")
		return nil
	}
	commChan := make(chan *protos.NetworkSliceResponse)
	go confClient.subscribeToConfigPod(commChan)
	return commChan
}

func CreateChannel(host string, timeout uint32) ConfClient {
	logger.GrpcLog.Infoln("create config client")
	// Second, check to see if we can reuse the gRPC connection for a new P4RT client
	conn, err := newClientConnection(host)
	if err != nil {
		logger.GrpcLog.Errorf("grpc connection failed %v", err)
		return nil
	}

	client := &ConfigClient{
		Client: protos.NewConfigServiceClient(conn),
		Conn:   conn,
		Host:   host,
	}

	return client
}

var kacp = keepalive.ClientParameters{
	Time:                20 * time.Second, // send pings every 20 seconds if there is no activity
	Timeout:             2 * time.Second,  // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

var retryPolicy = `{
		"methodConfig": [{
		  "name": [{"service": "grpc.Config"}],
		  "waitForReady": true,
		  "retryPolicy": {
			  "MaxAttempts": 4,
			  "InitialBackoff": ".01s",
			  "MaxBackoff": ".01s",
			  "BackoffMultiplier": 1.0,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }}]}`

func newClientConnection(host string) (conn *grpc.ClientConn, err error) {
	/* get connection */
	logger.GrpcLog.Infoln("Dial grpc connection - ", host)

	bd := 1 * time.Second
	mltpr := 1.0
	jitter := 0.2
	MaxDelay := 5 * time.Second
	bc := backoff.Config{BaseDelay: bd, Multiplier: mltpr, Jitter: jitter, MaxDelay: MaxDelay}

	crt := grpc.ConnectParams{Backoff: bc}
	dialOptions := []grpc.DialOption{grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp), grpc.WithDefaultServiceConfig(retryPolicy), grpc.WithConnectParams(crt)}
	conn, err = grpc.Dial(host, dialOptions...)
	if err != nil {
		logger.GrpcLog.Errorln("grpc dial err: ", err)
		return nil, err
	}
	// defer conn.Close()
	return conn, err
}

func (confClient *ConfigClient) GetConfigClientConn() *grpc.ClientConn {
	return confClient.Conn
}

func (confClient *ConfigClient) subscribeToConfigPod(commChan chan *protos.NetworkSliceResponse) {
	logger.GrpcLog.Infoln("subscribeToConfigPod ")
	myid := os.Getenv("HOSTNAME")
	var stream protos.ConfigService_NetworkSliceSubscribeClient
	for {
		if stream == nil {
			status := confClient.Conn.GetState()
			var err error
			if status == connectivity.Ready {
				logger.GrpcLog.Infoln("connectivity ready ")
				rreq := &protos.NetworkSliceRequest{RestartCounter: selfRestartCounter, ClientId: myid, MetadataRequested: confClient.MetadataRequested}
				if stream, err = confClient.Client.NetworkSliceSubscribe(context.Background(), rreq); err != nil {
					logger.GrpcLog.Errorf("Failed to subscribe: %v", err)
					time.Sleep(time.Second * 5)
					// Retry on failure
					continue
				}
			} else if status == connectivity.Idle {
				logger.GrpcLog.Errorf("Connectivity status idle, trying to connect again")
				time.Sleep(time.Second * 5)
				continue
			} else {
				logger.GrpcLog.Errorf("Connectivity status not ready")
				time.Sleep(time.Second * 5)
				continue
			}
		}
		rsp, err := stream.Recv()
		if err != nil {
			logger.GrpcLog.Errorf("Failed to receive message: %v", err)
			// Clearing the stream will force the client to resubscribe on next iteration
			stream = nil
			time.Sleep(time.Second * 5)
			// Retry on failure
			continue
		}

		logger.GrpcLog.Infoln("stream msg received ")
		logger.GrpcLog.Debugf("#Network Slices %v, RC of configpod %v ", len(rsp.NetworkSlice), rsp.RestartCounter)
		if configPodRestartCounter == 0 || (configPodRestartCounter == rsp.RestartCounter) {
			// first time connection or config update
			configPodRestartCounter = rsp.RestartCounter
			if len(rsp.NetworkSlice) > 0 {
				// always carries full config copy
				logger.GrpcLog.Infoln("First time config Received ", rsp)
				commChan <- rsp
			} else if rsp.ConfigUpdated == 1 {
				// config delete , all slices deleted
				logger.GrpcLog.Infoln("Complete config deleted ")
				commChan <- rsp
			}
		} else if len(rsp.NetworkSlice) > 0 {
			logger.GrpcLog.Errorf("Config received after config Pod restart")
			// config received after config pod restart
			configPodRestartCounter = rsp.RestartCounter
			commChan <- rsp
		} else {
			logger.GrpcLog.Errorf("Config Pod is restarted and no config received")
		}
	}
}

func readConfigInLoop(confClient *ConfigClient, commChan chan *protos.NetworkSliceResponse) {
	myid := os.Getenv("HOSTNAME")
	configReadTimeout := time.NewTicker(5000 * time.Millisecond)
	for {
		select {
		case <-configReadTimeout.C:
			status := confClient.Conn.GetState()
			if status == connectivity.Ready {
				rreq := &protos.NetworkSliceRequest{RestartCounter: selfRestartCounter, ClientId: myid, MetadataRequested: confClient.MetadataRequested}
				rsp, err := confClient.Client.GetNetworkSlice(context.Background(), rreq)
				if err != nil {
					logger.GrpcLog.Errorln("read Network Slice config from webconsole failed : ", err)
					continue
				}
				logger.GrpcLog.Debugf("#Network Slices %v, RC of configpod %v ", len(rsp.NetworkSlice), rsp.RestartCounter)
				if configPodRestartCounter == 0 || (configPodRestartCounter == rsp.RestartCounter) {
					// first time connection or config update
					configPodRestartCounter = rsp.RestartCounter
					if len(rsp.NetworkSlice) > 0 {
						// always carries full config copy
						logger.GrpcLog.Infoln("First time config Received ", rsp)
						commChan <- rsp
					} else if rsp.ConfigUpdated == 1 {
						// config delete , all slices deleted
						logger.GrpcLog.Infoln("Complete config deleted ")
						commChan <- rsp
					}
				} else if len(rsp.NetworkSlice) > 0 {
					logger.GrpcLog.Errorf("Config received after config Pod restart")
					// config received after config pod restart
					configPodRestartCounter = rsp.RestartCounter
					commChan <- rsp
				} else {
					logger.GrpcLog.Errorf("Config Pod is restarted and no config received")
				}
			} else {
				logger.GrpcLog.Errorln("read Network Slice config from webconsole skipped. GRPC channel down ")
			}
		}
	}
}
