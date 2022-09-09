// Copyright 2021 Harness Inc. All rights reserved.
// Use of this source code is governed by the Polyform Free Trial License
// that can be found in the LICENSE.md file for this repository.

package users

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gotidy/ptr"
	"github.com/harness/gitness/internal/api/render"
	"github.com/harness/gitness/internal/store"
	"github.com/harness/gitness/types"
	"github.com/harness/gitness/types/check"
	"github.com/harness/gitness/types/errs"
	"github.com/rs/zerolog/hlog"

	"github.com/go-chi/chi"
	"golang.org/x/crypto/bcrypt"
)

// GenerateFromPassword returns the bcrypt hash of the
// password at the given cost.
var hashPassword = bcrypt.GenerateFromPassword

// HandleUpdate returns an http.HandlerFunc that processes an http.Request
// to update a user account.
func HandleUpdate(users store.UserStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log := hlog.FromRequest(r)

		key := chi.URLParam(r, "user")
		user, err := users.FindKey(ctx, key)
		if errors.Is(err, errs.ResourceNotFound) {
			render.NotFoundf(w, "User not found.")
			return
		} else if err != nil {
			log.Err(err).Msgf("Failed to get user using key '%s'.", key)

			render.InternalError(w, errs.Internal)
			return
		}

		in := new(types.UserInput)
		if err := json.NewDecoder(r.Body).Decode(in); err != nil {
			render.BadRequestf(w, "Invalid request body: %s.", err)
			return
		}

		if in.Password != nil {
			hash, err := hashPassword([]byte(ptr.ToString(in.Password)), bcrypt.DefaultCost)
			if err != nil {
				log.Err(err).
					Int64("user_id", user.ID).
					Str("user_email", user.Email).
					Msg("Failed to hash password")

				render.InternalError(w, errs.Internal)
				return
			}
			user.Password = string(hash)
		}

		if in.Name != nil {
			user.Name = ptr.ToString(in.Name)
		}

		if in.Company != nil {
			user.Company = ptr.ToString(in.Company)
		}

		if in.Admin != nil {
			user.Admin = ptr.ToBool(in.Admin)
		}

		// TODO: why are we overwriting the password twice?
		if in.Password != nil {
			hash, err := bcrypt.GenerateFromPassword([]byte(ptr.ToString(in.Password)), bcrypt.DefaultCost)
			if err != nil {
				log.Err(err).
					Int64("user_id", user.ID).
					Str("user_email", user.Email).
					Msg("Failed to hash password")

				render.InternalError(w, errs.Internal)
				return
			}
			user.Password = string(hash)
		}

		if ok, err := check.User(user); !ok {
			log.Debug().Err(err).
				Int64("user_id", user.ID).
				Str("user_email", user.Email).
				Msg("invalid user input")

			render.BadRequest(w, err)
			return
		}

		user.Updated = time.Now().UnixMilli()

		err = users.Update(ctx, user)
		if err != nil {
			log.Err(err).
				Int64("user_id", user.ID).
				Str("user_email", user.Email).
				Msg("Failed to update the usser")

			render.InternalError(w, errs.Internal)
			return
		}

		render.JSON(w, user, 200)
	}
}