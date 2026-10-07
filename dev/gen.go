package dev

//go:generate go tool controller-gen object paths=../api/...
//go:generate go tool controller-gen crd paths=../api/... output:crd:artifacts:config=../config/crd
