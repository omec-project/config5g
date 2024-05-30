// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"os"
	"time"

	formatter "github.com/antonfisher/nested-logrus-formatter"
	"github.com/omec-project/util/logger"
	"github.com/omec-project/util/logger_conf"
	"github.com/sirupsen/logrus"
)

var (
	log     *logrus.Logger
	CfgLog  *logrus.Entry
	InitLog *logrus.Entry
	GrpcLog *logrus.Entry
)

func init() {
	log = logrus.New()
	log.SetReportCaller(false)

	log.Formatter = &formatter.Formatter{
		TimestampFormat: time.RFC3339,
		TrimMessages:    true,
		NoFieldsSpace:   true,
		HideKeys:        true,
		FieldsOrder:     []string{"component", "category"},
	}

	free5gcLogHook, err := logger.NewFileHook(logger_conf.Free5gcLogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o666)
	if err == nil {
		log.Hooks.Add(free5gcLogHook)
	}

	selfLogHook, err := logger.NewFileHook(logger_conf.NfLogDir+"nssf.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o666)
	if err == nil {
		log.Hooks.Add(selfLogHook)
	}

	CfgLog = log.WithFields(logrus.Fields{"component": "Config5g", "category": "CFG"})
	GrpcLog = log.WithFields(logrus.Fields{"component": "Config5g", "category": "GRPC"})
}

func SetLogLevel(level logrus.Level) {
	log.SetLevel(level)
}

func SetReportCaller(set bool) {
	log.SetReportCaller(set)
}
