//go:build cgo

package main

const defaultDBOptions = "_journal=WAL&_timeout=5000&_sync=NORMAL"

func initDynamicFlags() {
	extensions = flagSet.StringLong("extensions", "", "Comma-separated list of SQLite extensions to load")
}
