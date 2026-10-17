#!/bin/bash

ps ax | grep -v grep | grep adb | awk '{print $1}' | xargs kill 2>/dev/null
ps ax | grep -v grep | grep atalanta-master | awk '{print $1}' | xargs kill 2>/dev/null
ps ax | grep -v grep | grep atalanta-rs | awk '{print $1}' | xargs kill 2>/dev/null
ps ax | grep -v grep | grep atalanta-web-ui | awk '{print $1}' | xargs kill 2>/dev/null

sleep 1
ps ax | grep -v grep | grep adb | awk '{print $1}' | xargs kill -9 2>/dev/null
ps ax | grep -v grep | grep atalanta-master | awk '{print $1}' | xargs kill -9 2>/dev/null
ps ax | grep -v grep | grep atalanta-rs | awk '{print $1}' | xargs kill -9 2>/dev/null
ps ax | grep -v grep | grep atalanta-web-ui | awk '{print $1}' | xargs kill -9 2>/dev/null

echo KILLED ALL
