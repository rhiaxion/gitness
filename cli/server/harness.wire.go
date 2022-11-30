// Copyright 2021 Harness Inc. All rights reserved.
// Use of this source code is governed by the Polyform Free Trial License
// that can be found in the LICENSE.md file for this repository.

//go:build wireinject && harness
// +build wireinject,harness

package server

import (
	"context"

	"github.com/harness/gitness/gitrpc"
	gitrpcserver "github.com/harness/gitness/gitrpc/server"
	"github.com/harness/gitness/harness/auth/authn"
	"github.com/harness/gitness/harness/auth/authz"
	"github.com/harness/gitness/harness/bootstrap"
	"github.com/harness/gitness/harness/client"
	"github.com/harness/gitness/harness/router"
	"github.com/harness/gitness/harness/store"
	"github.com/harness/gitness/harness/types"
	"github.com/harness/gitness/harness/types/check"
	"github.com/harness/gitness/internal/api/controller/pullreq"
	"github.com/harness/gitness/internal/api/controller/repo"
	"github.com/harness/gitness/internal/api/controller/service"
	"github.com/harness/gitness/internal/api/controller/serviceaccount"
	"github.com/harness/gitness/internal/api/controller/space"
	"github.com/harness/gitness/internal/api/controller/user"
	"github.com/harness/gitness/internal/cron"
	"github.com/harness/gitness/internal/server"
	"github.com/harness/gitness/internal/store/database"
	"github.com/harness/gitness/internal/store/memory"
	gitnessTypes "github.com/harness/gitness/types"

	"github.com/google/wire"
)

func initSystem(ctx context.Context, config *gitnessTypes.Config) (*system, error) {
	wire.Build(
		newSystem,
		PackageConfigsWireSet,
		bootstrap.WireSet,
		database.WireSet,
		memory.WireSet,
		server.WireSet,
		cron.WireSet,
		space.WireSet,
		repo.WireSet,
		pullreq.WireSet,
		user.WireSet,
		service.WireSet,
		serviceaccount.WireSet,
		gitrpcserver.WireSet,
		gitrpc.WireSet,
		types.LoadConfig,
		router.WireSet,
		authn.WireSet,
		authz.WireSet,
		client.WireSet,
		store.WireSet,
		check.WireSet,
	)
	return &system{}, nil
}