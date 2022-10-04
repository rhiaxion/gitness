// Copyright 2022 Harness Inc. All rights reserved.
// Use of this source code is governed by the Polyform Free Trial License
// that can be found in the LICENSE.md file for this repository.

package enum

// ResourceType represents the different types of resources that can be guarded with permissions.
type ResourceType string

const (
	ResourceTypeSpace          ResourceType = "SPACE"
	ResourceTypeRepo           ResourceType = "REPOSITORY"
	ResourceTypeUser           ResourceType = "USER"
	ResourceTypeServiceAccount ResourceType = "SERVICEACCOUNT"
	//   ResourceType_Branch ResourceType = "BRANCH"
)

// Permission represents the different types of permissions a principal can have.
type Permission string

const (
	/*
	   ----- SPACE -----
	*/
	PermissionSpaceCreate Permission = "space_create"
	PermissionSpaceView   Permission = "space_view"
	PermissionSpaceEdit   Permission = "space_edit"
	PermissionSpaceDelete Permission = "space_delete"
)

const (
	/*
		----- REPOSITORY -----
	*/
	PermissionRepoCreate Permission = "repository_create"
	PermissionRepoView   Permission = "repository_view"
	PermissionRepoEdit   Permission = "repository_edit"
	PermissionRepoDelete Permission = "repository_delete"
)

const (
	/*
		----- USER -----
	*/
	PermissionUserCreate    Permission = "user_create"
	PermissionUserView      Permission = "user_view"
	PermissionUserEdit      Permission = "user_edit"
	PermissionUserDelete    Permission = "user_delete"
	PermissionUserEditAdmin Permission = "user_editadmin"
)

const (
	/*
		----- REPOSITORY -----
	*/
	PermissionServiceAccountCreate Permission = "serviceaccount_create"
	PermissionServiceAccountView   Permission = "serviceaccount_view"
	PermissionServiceAccountEdit   Permission = "serviceaccount_edit"
	PermissionServiceAccountDelete Permission = "serviceaccount_delete"
)