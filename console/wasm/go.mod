module github.com/donutnomad/tenuto/console/wasm

go 1.27

toolchain go1.27.4

require (
	github.com/donutnomad/tenuto v0.0.0
	github.com/donutnomad/wlang v0.0.0-20261112195913-46182b15d49c
)

require github.com/google/uuid v1.6.0 // indirect

replace github.com/donutnomad/tenuto => ../..
