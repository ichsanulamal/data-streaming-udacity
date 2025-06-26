#!/bin/bash
kill -9 `ps aux | grep config | grep -v grep | awk '{print $2}'`
