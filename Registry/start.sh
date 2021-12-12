#!/usr/bin/env bash

gunicorn -w 40 -b :8092 main:app


# pip install gunicorn
