module github.com/donutnomad/tenuto/console/wasm

go 1.27

toolchain go1.27.4

require (
	github.com/donutnomad/tenuto v0.0.0
	github.com/donutnomad/wlang v0.0.0-20261127005209-c674254fc863
)

require github.com/google/uuid v1.6.0 // indirect

replace github.com/donutnomad/tenuto => ../..
