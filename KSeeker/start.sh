#!/usr/bin/env bash

gunicorn -w 40 -b :9091 main:app


# pip install gunicorn
