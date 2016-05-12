#!/usr/bin/env bash
gunicorn -b 127.0.0.1:3000 pings:app
