//go:build !cgo

package main

const defaultDBOptions = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=sync(NORMAL)"

func initDynamicFlags() {}
