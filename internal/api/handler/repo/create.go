// Copyright 2021 Harness Inc. All rights reserved.
// Use of this source code is governed by the Polyform Free Trial License
// that can be found in the LICENSE.md file for this repository.

package repo

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/harness/gitness/internal/api/guard"
	"github.com/harness/gitness/internal/api/render"
	"github.com/harness/gitness/internal/api/request"
	"github.com/harness/gitness/internal/paths"
	"github.com/harness/gitness/internal/store"
	"github.com/harness/gitness/types"
	"github.com/harness/gitness/types/check"
	"github.com/harness/gitness/types/enum"
	"github.com/harness/gitness/types/errs"
	"github.com/rs/zerolog/hlog"
)

type repoCreateInput struct {
	Name        string `json:"name"`
	SpaceId     int64  `json:"spaceId"`
	DisplayName string `json:"displayName"`
	Description string `json:"description"`
	IsPublic    bool   `json:"isPublic"`
	ForkId      int64  `json:"forkId"`
}

/*
 * HandleCreate returns an http.HandlerFunc that creates a new repository.
 */
func HandleCreate(guard *guard.Guard, spaces store.SpaceStore, repos store.RepoStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log := hlog.FromRequest(r)

		in := new(repoCreateInput)
		err := json.NewDecoder(r.Body).Decode(in)
		if err != nil {
			log.Debug().Err(err).
				Msg("Decoding json body failed.")

			render.BadRequestf(w, "Invalid Request Body: %s.", err)
			return
		}

		// ensure we reference a space
		if in.SpaceId <= 0 {
			render.BadRequestf(w, "A repository can only be created within a space.")
			return
		}

		parentSpace, err := spaces.Find(ctx, in.SpaceId)
		if errors.Is(err, errs.ResourceNotFound) {
			render.NotFoundf(w, "Provided space wasn't found.")
			return
		} else if err != nil {
			log.Err(err).Msgf("Failed to get space with id '%s'.", in.SpaceId)

			render.InternalError(w, errs.Internal)
			return
		}

		// parentPath is assumed to be valid, in.Name gets validated in check.Repo function
		parentPath := parentSpace.Path

		/*
		 * AUTHORIZATION
		 * Create is a special case - check permission without specific resource
		 */
		scope := &types.Scope{SpacePath: parentPath}
		resource := &types.Resource{
			Type: enum.ResourceTypeRepo,
			Name: "",
		}
		if !guard.Enforce(w, r, scope, resource, enum.PermissionRepoCreate) {
			return
		}

		// get current user (safe to be there, or enforce would fail)
		usr, _ := request.UserFrom(ctx)

		// create new repo object
		repo := &types.Repository{
			Name:        strings.ToLower(in.Name),
			SpaceId:     in.SpaceId,
			DisplayName: in.DisplayName,
			Description: in.Description,
			IsPublic:    in.IsPublic,
			CreatedBy:   usr.ID,
			Created:     time.Now().UnixMilli(),
			Updated:     time.Now().UnixMilli(),
			ForkId:      in.ForkId,
		}

		// validate repo
		if err := check.Repo(repo); err != nil {
			render.BadRequest(w, err)
			return
		}

		// validate path (Due to racing conditions we can't be 100% sure on the path here, but that's okay)
		path := paths.Concatinate(parentPath, repo.Name)
		if err = check.PathParams(path, false); err != nil {
			render.BadRequest(w, err)
			return
		}

		// create in store
		err = repos.Create(ctx, repo)
		if errors.Is(err, errs.Duplicate) {
			log.Warn().Err(err).
				Msg("Repository creation failed as a duplicate was detected.")

			render.BadRequestf(w, "Path '%s' already exists.", path)
			return
		} else if err != nil {
			log.Error().Err(err).
				Msg("Repository creation failed.")

			render.InternalError(w, errs.Internal)
			return
		}

		render.JSON(w, repo, 200)
	}
}