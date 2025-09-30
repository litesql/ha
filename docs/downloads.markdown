---
layout: page
title: Downloads
permalink: /downloads/
---

## Download the latest HA Release

| Operational System | Architecture |  |
|--------------------|------|------|
| Linux              | amd64 (x86)| [ha_Linux_x86_64.tar.gz](https://github.com/litesql/ha/releases/latest/download/ha_Linux_x86_64.tar.gz) |
| Linux              | arm64 | [ha_Linux_arm64.tar.gz](https://github.com/litesql/ha/releases/latest/download/ha_Linux_arm64.tar.gz) |
| MacOS              | amd64 (x86)| [ha_Darwin_x86_64.tar.gz](https://github.com/litesql/ha/releases/latest/download/ha_Darwin_x86_64.tar.gz) |
| MacOS              | arm64 | [ha_Darwin_arm64.tar.gz](https://github.com/litesql/ha/releases/latest/download/ha_Darwin_arm64.tar.gz) |
| Windows            | amd64 (x86) | [ha_Windows_x86_64.zip](https://github.com/litesql/ha/releases/latest/download/ha_Windows_x86_64.zip) |

## Install from source

```sh
git clone https://github.com/litesql/ha.git
cd ha
go install
```

## Using docker

```sh
docker run --name ha \
-e HA_MEMORY=true \
-p 5432:5432 -p 8080:8080 -p 4222:4222 \
ghcr.io/litesql/ha:latest
```

- Set up a volume at /data to store the NATS streams state.